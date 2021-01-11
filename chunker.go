package chunker

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/dgraph-io/dgo/v2/protos/api"
	"github.com/dgraph-io/dgraph/types"
	"github.com/dgraph-io/dgraph/types/facets"
	"github.com/minio/simdjson-go"
)

type Quad struct {
	Subject   string
	Predicate string
	ObjectId  string
	ObjectVal interface{}
	Facets    []*api.Facet
}

func NewQuad() *Quad {
	return &Quad{Facets: make([]*api.Facet, 0)}
}

type ParserLevels struct {
	Counter uint64
	Levels  []*ParserLevel
}

type ParserLevel struct {
	Array   bool
	Subject string
	Wait    *Quad
	Scalars bool
}

func NewParserLevels() *ParserLevels {
	return &ParserLevels{
		Levels: make([]*ParserLevel, 0),
	}
}

func (p *ParserLevels) Pop() *ParserLevel {
	if len(p.Levels) == 0 {
		return nil
	}
	l := p.Levels[len(p.Levels)-1]
	p.Levels = p.Levels[:len(p.Levels)-1]
	return l
}

func (p *ParserLevels) Get(n int) *ParserLevel {
	if len(p.Levels) <= n {
		return nil
	}
	return p.Levels[len(p.Levels)-1-n]
}

func (p *ParserLevels) InArray() bool {
	if len(p.Levels) < 2 {
		return false
	}
	return p.Levels[len(p.Levels)-2].Array
}

// Deeper is called when we encounter a '{' or '[' node and are going "deeper"
// into the nested JSON objects. It's important to set the 'array' param to
// true when we encounter '[' nodes because we only want to increment the
// global Subject counter for objects.
func (p *ParserLevels) Deeper(array bool) *ParserLevel {
	var subject string
	if !array {
		p.Counter++
		// TODO: use dgraph prefix and random number
		subject = fmt.Sprintf("c.%d", p.Counter)
	}
	level := &ParserLevel{
		Array:   array,
		Subject: subject,
	}
	p.Levels = append(p.Levels, level)
	return level
}

// Subject returns the current subject based on how deeply nested we are. We
// iterate through the Levels in reverse order (it's a stack) to find a
// non-array Level with a subject.
func (p *ParserLevels) Subject() string {
	for i := len(p.Levels) - 1; i >= 0; i-- {
		if !p.Levels[i].Array {
			return p.Levels[i].Subject
		}
	}
	return ""
}

// FoundSubject is called when the Parser is in the Uid state and finds a valid
// uid.
func (p *ParserLevels) FoundSubject(s string) {
	p.Levels[len(p.Levels)-1].Subject = s
}

type ParserState func(simdjson.Tag) (ParserState, error)

type Parser struct {
	Cursor         uint64
	StringCursor   uint64
	SubjectCounter uint64
	Quad           *Quad
	Facet          *api.Facet
	Quads          []*Quad
	Levels         *ParserLevels
	Parsed         *simdjson.ParsedJson
	FacetPred      string
	FacetId        int

	Iter simdjson.Iter
}

func NewParser() *Parser {
	return &Parser{
		Cursor: 1,
		Quad:   NewQuad(),
		Quads:  make([]*Quad, 0),
		Levels: NewParserLevels(),
		Facet:  &api.Facet{},
	}
}

func (p *Parser) Run(d []byte) (err error) {
	if p.Parsed, err = simdjson.Parse(d, nil); err != nil {
		return
	}
	p.Iter = p.Parsed.Iter()
	p.Iter.AdvanceInto()
	for state := p.Root; state != nil; {
		if state, err = state(p.Iter.AdvanceInto()); err != nil {
			return
		}
	}
	return
}

// Root is the initial state of the Parser. It should only look for '{' or '['
// nodes, anything else is bad JSON.
func (p *Parser) Root(t simdjson.Tag) (ParserState, error) {
	switch t {
	case simdjson.TagObjectStart:
		p.Levels.Deeper(false)
		return p.Object, nil
	case simdjson.TagArrayStart:
		p.Levels.Deeper(true)
		return p.Array, nil
	}
	return nil, nil
}

// Object is the most common state for the Parser to be in--we're usually in an
// object of some kind.
func (p *Parser) Object(t simdjson.Tag) (ParserState, error) {
	switch t {
	case simdjson.TagObjectStart:
		p.Levels.Deeper(false)
		return p.Object, nil
	case simdjson.TagObjectEnd:
		l := p.Levels.Get(0)
		// check if the current level has anything waiting to be pushed, if the
		// current level is scalars we don't push anything
		if l.Wait != nil && !l.Scalars {
			p.Quad = l.Wait
			p.Quad.ObjectId = l.Subject
			p.Quads = append(p.Quads, p.Quad)
			p.Quad = NewQuad()
		} else {
			if p.Levels.InArray() {
				a := p.Levels.Get(1)
				if a.Array && a.Wait != nil && !a.Scalars {
					p.Quad.Subject = a.Wait.Subject
					p.Quad.Predicate = a.Wait.Predicate
					p.Quad.ObjectId = l.Subject
					p.Quad.Facets = a.Wait.Facets
					p.Quads = append(p.Quads, p.Quad)
					p.Quad = NewQuad()
				}
			}
		}
		p.Levels.Pop()
		return p.Object, nil
	case simdjson.TagArrayEnd:
		p.Levels.Pop()
		return p.Object, nil
	case simdjson.TagString:
		s, err := p.Iter.String()
		if err != nil {
			return nil, err
		}
		if s == "uid" {
			return p.Uid, nil
		}
		// check if this is a facet definition
		if strings.Contains(s, "|") {
			e := strings.Split(s, "|")
			if len(e) == 2 {
				p.FacetPred = e[0]
				p.Facet.Key = e[1]
				/* TODO
				// peek at the next node to see if it's a scalar facet or map
				next := byte(p.Parsed.Tape[p.Cursor+1] >> 56)
				if next == '{' {
					// go into the object so MapFacet can immediately check the
					// keys
					p.Cursor++
					return p.MapFacet, nil
				}
				*/
				return p.ScalarFacet, nil
			}
		} else {
			// found a normal nquad
			p.Quad.Subject = p.Levels.Subject()
			p.Quad.Predicate = s
			return p.Value, nil
		}
		// not sure what this string is, try again
		return p.Object, nil
	}
	return nil, nil
}

func (p *Parser) MapFacet(t simdjson.Tag) (ParserState, error) {
	// map facet keys must be (numerical) strings
	if t != simdjson.TagString {
		return p.Object, nil
	}
	s, err := p.Iter.String()
	if err != nil {
		return nil, err
	}
	id, err := strconv.Atoi(s)
	if err != nil {
		return nil, err
	}
	p.FacetId = id
	return p.MapFacetVal, nil
}

func (p *Parser) MapFacetVal(t simdjson.Tag) (ParserState, error) {
	var f *api.Facet
	var err error
	var facetVal interface{}

	switch t {
	case simdjson.TagString:
		s, err := p.Iter.String()
		if err != nil {
			return nil, err
		}
		ti, err := types.ParseTime(s)
		if err == nil {
			p.Facet.ValType = api.Facet_DATETIME
			facetVal = ti
		} else {
			if f, err = facets.FacetFor(p.Facet.Key, strconv.Quote(s)); err != nil {
				return nil, err
			}
			p.Facet = f
			goto done
		}
	case simdjson.TagUint:
		// NOTE: dgraph doesn't have uint64 facet type, so we just convert it to
		//       int64
		fallthrough
	case simdjson.TagInteger:
		p.Facet.ValType = api.Facet_INT
		if facetVal, err = p.Iter.Int(); err != nil {
			return nil, err
		}
	case simdjson.TagFloat:
		p.Facet.ValType = api.Facet_FLOAT
		if facetVal, err = p.Iter.Float(); err != nil {
			return nil, err
		}
	case simdjson.TagBoolTrue:
		p.Facet.ValType = api.Facet_BOOL
		facetVal = true
	case simdjson.TagBoolFalse:
		p.Facet.ValType = api.Facet_BOOL
		facetVal = false
	case simdjson.TagNull:
		// TODO: can facets have null value?
		return p.MapFacet, nil
	}

	if f, err = facets.ToBinary(p.Facet.Key, facetVal, p.Facet.ValType); err != nil {
		return nil, err
	}
	p.Facet = f

done:
	// TODO: move this to a cache so we only have to grab referenced quads once
	//       per facet map definition, rather than for each index-value
	//
	// find every quad that could be referenced by the facet
	quads := make([]*Quad, 0)
	for i := len(p.Quads) - 1; i >= 0; i-- {
		if p.Quads[i].Predicate == p.FacetPred {
			quads = append(quads, p.Quads[i])
			/*
				// TODO: if we want to only allow map facet definitions directly
				//       under the quad definition, uncomment this
				} else {
					break
			*/
		}
	}
	for i := len(quads) - 1; i >= 0; i-- {
		if i == len(quads)-1-p.FacetId {
			quads[i].Facets = append(quads[i].Facets, p.Facet)
			// make new facet
			facetKey := p.Facet.Key
			p.Facet = &api.Facet{Key: facetKey}
			return p.MapFacet, nil
		}
	}
	return p.MapFacet, nil
}

func (p *Parser) ScalarFacet(t simdjson.Tag) (ParserState, error) {
	var f *api.Facet
	var err error
	var facetVal interface{}

	switch t {
	case simdjson.TagString:
		s, err := p.Iter.String()
		if err != nil {
			return nil, err
		}
		ti, err := types.ParseTime(s)
		if err == nil {
			p.Facet.ValType = api.Facet_DATETIME
			facetVal = ti
		} else {
			if f, err = facets.FacetFor(p.Facet.Key, strconv.Quote(s)); err != nil {
				return nil, err
			}
			p.Facet = f
			goto done
		}
	case simdjson.TagUint:
		// NOTE: dgraph doesn't have uint64 facet type, so we just convert it to
		//       int64
		fallthrough
	case simdjson.TagInteger:
		p.Facet.ValType = api.Facet_INT
		if facetVal, err = p.Iter.Int(); err != nil {
			return nil, err
		}
	case simdjson.TagFloat:
		p.Facet.ValType = api.Facet_FLOAT
		if facetVal, err = p.Iter.Float(); err != nil {
			return nil, err
		}
	case simdjson.TagBoolTrue:
		p.Facet.ValType = api.Facet_BOOL
		facetVal = true
	case simdjson.TagBoolFalse:
		p.Facet.ValType = api.Facet_BOOL
		facetVal = false
	case simdjson.TagNull:
		// TODO: can facets have null value?
		return p.MapFacet, nil
	}

	if f, err = facets.ToBinary(p.Facet.Key, facetVal, p.Facet.ValType); err != nil {
		return nil, err
	}
	p.Facet = f

done:
	for i := len(p.Levels.Levels) - 1; i >= 0; i-- {
		if p.Levels.Levels[i].Wait != nil && p.Levels.Levels[i].Wait.Predicate == p.FacetPred {
			p.Levels.Levels[i].Wait.Facets = append(p.Levels.Levels[i].Wait.Facets, p.Facet)
			p.Facet = &api.Facet{}
			return p.Object, nil
		}
	}
	for i := len(p.Quads) - 1; i >= 0; i-- {
		if p.Quads[i].Predicate == p.FacetPred {
			p.Quads[i].Facets = append(p.Quads[i].Facets, p.Facet)
			p.Facet = &api.Facet{}
			return p.Object, nil
		}
	}
	return p.Object, nil
}

func (p *Parser) Array(t simdjson.Tag) (ParserState, error) {
	var err error

	l := p.Levels.Get(0)
	if l.Wait != nil {
		p.Quad.Subject = l.Wait.Subject
		p.Quad.Predicate = l.Wait.Predicate
	}
	switch t {
	case simdjson.TagObjectStart:
		p.Levels.Deeper(false)
		return p.Object, nil
	case simdjson.TagObjectEnd:
		return p.Object, nil
	case simdjson.TagArrayStart:
		p.Levels.Deeper(false)
		return p.Array, nil
	case simdjson.TagArrayEnd:
		return p.Object, nil
	case simdjson.TagString:
		l.Scalars = true
		if p.Quad.ObjectVal, err = p.Iter.String(); err != nil {
			return nil, err
		}
	case simdjson.TagUint:
		fallthrough
	case simdjson.TagInteger:
		l.Scalars = true
		if p.Quad.ObjectVal, err = p.Iter.Int(); err != nil {
			return nil, err
		}
	case simdjson.TagFloat:
		l.Scalars = true
		if p.Quad.ObjectVal, err = p.Iter.Float(); err != nil {
			return nil, err
		}
	case simdjson.TagBoolTrue:
		l.Scalars = true
		p.Quad.ObjectVal = true
	case simdjson.TagBoolFalse:
		l.Scalars = true
		p.Quad.ObjectVal = false
	case simdjson.TagNull:
		l.Scalars = true
		p.Quad.ObjectVal = nil
	}
	p.Quads = append(p.Quads, p.Quad)
	p.Quad = NewQuad()
	return p.Array, nil
}

func (p *Parser) Value(t simdjson.Tag) (ParserState, error) {
	var err error

	switch t {
	case simdjson.TagObjectStart:
		l := p.Levels.Deeper(false)
		l.Wait = p.Quad
		p.Quad = NewQuad()
		return p.Object, nil
	case simdjson.TagArrayStart:
		l := p.Levels.Deeper(true)
		l.Wait = p.Quad
		p.Quad = NewQuad()
		return p.Array, nil
	case simdjson.TagString:
		if p.Quad.ObjectVal, err = p.Iter.String(); err != nil {
			return nil, err
		}
	case simdjson.TagUint:
		fallthrough
	case simdjson.TagInteger:
		if p.Quad.ObjectVal, err = p.Iter.Int(); err != nil {
			return nil, err
		}
	case simdjson.TagFloat:
		if p.Quad.ObjectVal, err = p.Iter.Float(); err != nil {
			return nil, err
		}
	case simdjson.TagBoolTrue:
		p.Quad.ObjectVal = true
	case simdjson.TagBoolFalse:
		p.Quad.ObjectVal = false
	case simdjson.TagNull:
		p.Quad.ObjectVal = nil
	}
	p.Quads = append(p.Quads, p.Quad)
	p.Quad = NewQuad()
	return p.Object, nil
}

func (p *Parser) Uid(t simdjson.Tag) (ParserState, error) {
	if t != simdjson.TagString {
		return nil, errors.New("expected uid, instead found: " + fmt.Sprintf("%v", t))
	}
	s, err := p.Iter.String()
	if err != nil {
		return nil, err
	}
	p.Levels.FoundSubject(s)
	return p.Object, nil
}
