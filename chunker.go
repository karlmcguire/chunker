package chunker

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"strconv"
	"strings"

	"github.com/davecgh/go-spew/spew"
	"github.com/dgraph-io/dgo/v2/protos/api"
	"github.com/dgraph-io/dgraph/types"
	"github.com/dgraph-io/dgraph/types/facets"
	json "github.com/minio/simdjson-go"
)

type Quad struct {
	Subject   string
	Predicate string
	ObjectId  string
	ObjectVal interface{}
	Facets    []*api.Facet
}

func NewQuad() *Quad {
	return &Quad{
		Facets: make([]*api.Facet, 0),
	}
}

var nextUid uint64 = 0

func getNextUid() string {
	nextUid++
	return fmt.Sprintf("c.%d", nextUid)
}

type (
	LevelType uint8
	Level     struct {
		Type    LevelType
		Subject string
		Wait    *Quad
		Scalars bool
	}
)

const (
	OBJECT LevelType = iota
	ARRAY
)

func (t LevelType) String() string {
	switch t {
	case OBJECT:
		return "OBJECT"
	case ARRAY:
		return "ARRAY"
	}
	return "?"
}

type (
	ParserState func(byte) (ParserState, error)
	Parser      struct {
		Cursor       uint64
		StringCursor uint64
		Quad         *Quad
		Facet        *api.Facet
		Quads        []*Quad
		Levels       []*Level
		Parsed       *json.ParsedJson
		FacetPred    string
		FacetId      int
	}
)

func NewParser() *Parser {
	return &Parser{
		Cursor: 1,
		Quad:   NewQuad(),
		Quads:  make([]*Quad, 0),
		Levels: make([]*Level, 0),
		Facet:  &api.Facet{},
	}
}

func (p *Parser) Run(d []byte) (err error) {
	if p.Parsed, err = json.Parse(d, nil); err != nil {
		return
	}
	for state := p.Root; state != nil; p.Cursor++ {
		if p.Cursor >= uint64(len(p.Parsed.Tape)) {
			return
		}
		//p.Log(state)
		if state, err = state(byte(p.Parsed.Tape[p.Cursor] >> 56)); err != nil {
			return
		}
	}
	return
}

func (p *Parser) Log(state ParserState) {
	line := runtime.FuncForPC(reflect.ValueOf(state).Pointer()).Name()
	name := strings.Split(strings.Split(line, ".")[3], "-")
	fmt.Printf("-> %c - %v\n%v\n",
		p.Parsed.Tape[p.Cursor]>>56, name[0], spew.Sdump(p.Levels))
}

func (p *Parser) String() string {
	p.Cursor++
	length := p.Parsed.Tape[p.Cursor]
	s := p.Parsed.Strings[p.StringCursor : p.StringCursor+length]
	p.StringCursor += length
	return string(s)
}

func (p *Parser) Deeper(t LevelType) *Level {
	var subject string
	if t == OBJECT {
		subject = getNextUid()
	}
	level := &Level{
		Type:    t,
		Subject: subject,
	}
	p.Levels = append(p.Levels, level)
	return level
}

func (p *Parser) Subject() string {
	if len(p.Levels) == 0 {
		// NOTE: this should never happen
		return ""
	}
	for i := len(p.Levels) - 1; i >= 0; i-- {
		if p.Levels[i].Type == OBJECT {
			return p.Levels[i].Subject
		}
	}
	// NOTE: this should never happen
	return ""
}

func (p *Parser) Root(n byte) (ParserState, error) {
	switch n {
	case '{':
		p.Deeper(OBJECT)
		return p.Object, nil
	case '[':
		p.Deeper(ARRAY)
		return p.Array, nil
	}
	return nil, nil
}

func (p *Parser) Object(n byte) (ParserState, error) {
	switch n {
	case '{':
		p.Deeper(OBJECT)
		return p.Object, nil
	case '}':
		l := p.Levels[len(p.Levels)-1]
		if l.Wait != nil && !l.Scalars {
			p.Quad = l.Wait
			p.Quad.ObjectId = l.Subject
			p.Quads = append(p.Quads, p.Quad)
			p.Quad = NewQuad()
		} else {
			if len(p.Levels) >= 2 {
				a := p.Levels[len(p.Levels)-2]
				// TODO: cleanup
				if a.Type == ARRAY && a.Wait != nil && !a.Scalars {
					p.Quad.Subject = a.Wait.Subject
					p.Quad.Predicate = a.Wait.Predicate
					p.Quad.ObjectId = l.Subject
					p.Quad.Facets = a.Wait.Facets
					p.Quads = append(p.Quads, p.Quad)
					p.Quad = NewQuad()
				}
			}
		}
		p.Levels = p.Levels[:len(p.Levels)-1]
		return p.Object, nil
	case ']':
		p.Levels = p.Levels[:len(p.Levels)-1]
		return p.Object, nil
	case '"':
		s := p.String()
		if s == "uid" {
			return p.Uid, nil
		}
		if strings.Contains(s, "|") {
			e := strings.Split(s, "|")
			if len(e) == 2 {
				p.FacetPred = e[0]
				p.Facet.Key = e[1]
				// peek at the next node
				next := byte(p.Parsed.Tape[p.Cursor+1] >> 56)
				if next == '{' {
					p.Cursor++
					return p.MapFacet, nil
				}
				return p.ScalarFacet, nil
			}
		} else {
			p.Quad.Subject = p.Subject()
			p.Quad.Predicate = s
			return p.Value, nil
		}
		return p.Object, nil
	}
	return nil, nil
}

func (p *Parser) MapFacet(n byte) (ParserState, error) {
	switch n {
	case '"':
		id, err := strconv.Atoi(p.String())
		if err != nil {
			return nil, err
		}
		p.FacetId = id
		return p.MapFacetVal, nil
	case '}':
		// TODO: document why this is important
		p.Facet = &api.Facet{}
	}
	return p.Object, nil
}

func (p *Parser) MapFacetVal(n byte) (ParserState, error) {
	var f *api.Facet
	var err error
	var facetVal interface{}

	switch n {
	case '"':
		s := p.String()
		t, err := types.ParseTime(s)
		if err == nil {
			p.Facet.ValType = api.Facet_DATETIME
			facetVal = t
		} else {
			/*
				p.Facet.ValType = api.Facet_STRING
				facetVal = s
			*/

			if f, err = facets.FacetFor(p.Facet.Key, strconv.Quote(s)); err != nil {
				return nil, err
			}
			p.Facet = f
			goto done
		}
	case 'u':
		// NOTE: dgraph doesn't have uint64 facet type, so we just convert it to
		//       int64
		fallthrough
	case 'l':
		p.Facet.ValType = api.Facet_INT
		p.Cursor++
		facetVal = int64(p.Parsed.Tape[p.Cursor])
	case 'd':
		p.Facet.ValType = api.Facet_FLOAT
		p.Cursor++
		facetVal = math.Float64frombits(p.Parsed.Tape[p.Cursor])
	case 't':
		p.Facet.ValType = api.Facet_BOOL
		facetVal = true
	case 'f':
		p.Facet.ValType = api.Facet_BOOL
		facetVal = false
	case 'n':
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

func (p *Parser) ScalarFacet(n byte) (ParserState, error) {
	var f *api.Facet
	var err error
	var facetVal interface{}

	switch n {
	case '"':
		s := p.String()
		t, err := types.ParseTime(s)
		if err == nil {
			p.Facet.ValType = api.Facet_DATETIME
			facetVal = t
		} else {
			if f, err = facets.FacetFor(p.Facet.Key, strconv.Quote(s)); err != nil {
				return nil, err
			}
			p.Facet = f
			goto done
		}
	case 'u':
		// NOTE: dgraph doesn't have uint64 facet type, so we just convert it to
		//       int64
		fallthrough
	case 'l':
		p.Facet.ValType = api.Facet_INT
		p.Cursor++
		facetVal = int64(p.Parsed.Tape[p.Cursor])
	case 'd':
		p.Facet.ValType = api.Facet_FLOAT
		p.Cursor++
		facetVal = math.Float64frombits(p.Parsed.Tape[p.Cursor])
	case 't':
		p.Facet.ValType = api.Facet_BOOL
		facetVal = true
	case 'f':
		p.Facet.ValType = api.Facet_BOOL
		facetVal = false
	case 'n':
		// TODO: can facets have null value?
		return p.MapFacet, nil
	}

	if f, err = facets.ToBinary(p.Facet.Key, facetVal, p.Facet.ValType); err != nil {
		return nil, err
	}
	p.Facet = f

done:
	for i := len(p.Levels) - 1; i >= 0; i-- {
		if p.Levels[i].Wait != nil && p.Levels[i].Wait.Predicate == p.FacetPred {
			p.Levels[i].Wait.Facets = append(p.Levels[i].Wait.Facets, p.Facet)
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

func (p *Parser) Array(n byte) (ParserState, error) {
	l := p.Levels[len(p.Levels)-1]
	if l.Wait != nil {
		p.Quad.Subject = l.Wait.Subject
		p.Quad.Predicate = l.Wait.Predicate
	}
	switch n {
	case '{':
		p.Deeper(OBJECT)
		return p.Object, nil
	case '}':
		return p.Object, nil
	case '[':
		p.Deeper(ARRAY)
		return p.Array, nil
	case ']':
		return p.Object, nil
	case '"':
		l.Scalars = true
		p.Quad.ObjectVal = p.String()
	case 'l':
		l.Scalars = true
		p.Cursor++
		p.Quad.ObjectVal = int64(p.Parsed.Tape[p.Cursor])
	case 'u':
		l.Scalars = true
		p.Cursor++
		p.Quad.ObjectVal = p.Parsed.Tape[p.Cursor]
	case 'd':
		l.Scalars = true
		p.Cursor++
		p.Quad.ObjectVal = math.Float64frombits(p.Parsed.Tape[p.Cursor])
	case 't':
		l.Scalars = true
		p.Quad.ObjectVal = true
	case 'f':
		l.Scalars = true
		p.Quad.ObjectVal = false
	case 'n':
		l.Scalars = true
		p.Quad.ObjectVal = nil
	}
	p.Quads = append(p.Quads, p.Quad)
	p.Quad = NewQuad()
	return p.Array, nil
}

func (p *Parser) Value(n byte) (ParserState, error) {
	switch n {
	case '{':
		if byte(p.Parsed.Tape[p.Cursor+1]>>56) == '}' {
			p.Cursor++
			return p.Object, nil
		}
		l := p.Deeper(OBJECT)
		l.Wait = p.Quad
		p.Quad = NewQuad()
		return p.Object, nil
	case '[':
		if byte(p.Parsed.Tape[p.Cursor+1]>>56) == ']' {
			p.Cursor++
			return p.Object, nil
		}
		l := p.Deeper(ARRAY)
		l.Wait = p.Quad
		p.Quad = NewQuad()
		return p.Array, nil
	case '"':
		p.Quad.ObjectVal = p.String()
	case 'l':
		p.Cursor++
		p.Quad.ObjectVal = int64(p.Parsed.Tape[p.Cursor])
	case 'u':
		p.Cursor++
		p.Quad.ObjectVal = p.Parsed.Tape[p.Cursor]
	case 'd':
		p.Cursor++
		p.Quad.ObjectVal = math.Float64frombits(p.Parsed.Tape[p.Cursor])
	case 't':
		p.Quad.ObjectVal = true
	case 'f':
		p.Quad.ObjectVal = false
	case 'n':
		p.Quad.ObjectVal = nil
	}
	p.Quads = append(p.Quads, p.Quad)
	p.Quad = NewQuad()
	return p.Object, nil
}

func (p *Parser) Uid(n byte) (ParserState, error) {
	if n != '"' {
		return nil, errors.New("expected uid, instead found: " + p.String())
	}
	p.Levels[len(p.Levels)-1].Subject = p.String()
	return p.Object, nil
}
