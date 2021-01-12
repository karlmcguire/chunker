package chunker

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/dgraph-io/dgo/v2/protos/api"
	"github.com/dgraph-io/dgraph/types"
	"github.com/dgraph-io/dgraph/types/facets"
	json "github.com/minio/simdjson-go"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/geojson"
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

type ParserState func(byte) (ParserState, error)

type Parser struct {
	Cursor       uint64
	StringCursor uint64
	Quad         *Quad
	Facet        *api.Facet
	Quads        []*Quad
	Levels       *ParserLevels
	Parsed       *json.ParsedJson
	FacetPred    string
	FacetId      int
	Iter         json.Iter
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
	if p.Parsed, err = json.Parse(d, nil); err != nil {
		return
	}
	p.Iter = p.Parsed.Iter()
	for state := p.Root; state != nil; p.Cursor++ {
		if p.Cursor >= uint64(len(p.Parsed.Tape)) {
			return
		}
		p.Iter.AdvanceInto()
		//t := p.Iter.AdvanceInto()
		//fmt.Printf("%v %d %c\n", t, p.Cursor, p.Parsed.Tape[p.Cursor]>>56)
		if state, err = state(byte(p.Parsed.Tape[p.Cursor] >> 56)); err != nil {
			return
		}
	}
	return
}

// String is called when we encounter a '"' (string) node and want to get the
// value from the string buffer. In the simdjson Tape, the string length is
// immediately after the '"' node, so we first have to increment the Cursor
// by one and then we use the Tape value as the string length, and create
// a byte slice from the string buffer.
func (p *Parser) String() string {
	p.Cursor++
	length := p.Parsed.Tape[p.Cursor]
	s := p.Parsed.Strings[p.StringCursor : p.StringCursor+length]
	p.StringCursor += length
	return string(s)
}

// Root is the initial state of the Parser. It should only look for '{' or '['
// nodes, anything else is bad JSON.
func (p *Parser) Root(n byte) (ParserState, error) {
	switch n {
	case '{':
		p.Levels.Deeper(false)
		return p.Object, nil
	case '[':
		p.Levels.Deeper(true)
		return p.Array, nil
	}
	return nil, nil
}

// Object is the most common state for the Parser to be in--we're usually in an
// object of some kind.
func (p *Parser) Object(n byte) (ParserState, error) {
	switch n {
	case '{':
		p.Levels.Deeper(false)
		return p.Object, nil
	case '}':
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
	case ']':
		p.Levels.Pop()
		return p.Object, nil
	case '"':
		s := p.String()
		if s == "uid" {
			return p.Uid, nil
		}
		// check if this is a facet definition
		if strings.Contains(s, "|") {
			e := strings.Split(s, "|")
			if len(e) == 2 {
				p.FacetPred = e[0]
				p.Facet.Key = e[1]
				// peek at the next node to see if it's a scalar facet or map
				next := byte(p.Parsed.Tape[p.Cursor+1] >> 56)
				if next == '{' {
					// go into the object so MapFacet can immediately check the
					// keys
					p.Cursor++
					p.Iter.AdvanceInto()
					return p.MapFacet, nil
				}
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

func (p *Parser) MapFacet(n byte) (ParserState, error) {
	// map facet keys must be (numerical) strings
	if n != '"' {
		return p.Object, nil
	}
	id, err := strconv.Atoi(p.String())
	if err != nil {
		return nil, err
	}
	p.FacetId = id
	return p.MapFacetVal, nil
}

func (p *Parser) MapFacetVal(n byte) (ParserState, error) {
	// getFacet fills the p.Facet struct
	if err := p.getFacet(n); err != nil {
		return nil, err
	}
	// TODO: move this to a cache so we only have to grab referenced quads once
	//       per facet map definition, rather than for each index-value
	//
	// find every quad that could be referenced by the facet
	quads := make([]*Quad, 0)
	for i := len(p.Quads) - 1; i >= 0; i-- {
		if p.Quads[i].Predicate == p.FacetPred {
			quads = append(quads, p.Quads[i])
		}
	}
	for i := len(quads) - 1; i >= 0; i-- {
		if i == len(quads)-1-p.FacetId {
			quads[i].Facets = append(quads[i].Facets, p.Facet)
			p.Facet = &api.Facet{}
			return p.MapFacet, nil
		}
	}
	return p.MapFacet, nil
}

func (p *Parser) ScalarFacet(n byte) (ParserState, error) {
	// getFacet fills the p.Facet struct
	if err := p.getFacet(n); err != nil {
		return nil, err
	}
	// because this is a scalar facet and you can reference parent quads, we
	// first have to check if any of the quads waiting on a Level match the
	// facet predicate
	if p.Levels.FoundScalarFacet(p.FacetPred, p.Facet) {
		return p.Object, nil
	}
	// we didn't find the predicate waiting on a Level, so go through quads
	// in reverse order (it's most likely that the referenced quad is near
	// the end of the p.Quads slice)
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
	// get current array level
	a := p.Levels.Get(0)
	// if this is a scalar array (we won't know until we get to the switch
	// statement and set a.Scalars = true) we'll need the waiting info so we can
	// generate a quad for each scalar
	//
	// for example:
	//
	//     "friend": ["karl", "megan", "sarah"]
	//
	// will generate three quads each with the same subject (unknown) and
	// predicate ("friend")
	if a.Wait != nil {
		p.Quad.Subject = a.Wait.Subject
		p.Quad.Predicate = a.Wait.Predicate
	}
	switch n {
	case '{':
		p.Levels.Deeper(false)
		return p.Object, nil
	case '}':
		return p.Object, nil
	case '[':
		p.Levels.Deeper(false)
		return p.Array, nil
	case ']':
		// return to Object rather than Array because it's the default state
		return p.Object, nil
	case '"', 'l', 'u', 'd', 't', 'f', 'n':
		a.Scalars = true
		p.getScalarValue(n)
	}
	return p.Array, nil
}

func (p *Parser) Value(n byte) (ParserState, error) {
	switch n {
	case '{':
		if p.isGeo() {
			// TODO: add predicate to Level wait
			if err := p.getGeoValue(); err != nil {
				return nil, err
			}
			return p.Object, nil
		}
		return p.openValueLevel('}', false, p.Object), nil
	case '[':
		return p.openValueLevel(']', true, p.Array), nil
	case '"', 'l', 'u', 'd', 't', 'f', 'n':
		p.getScalarValue(n)
	}
	return p.Object, nil
}

// Uid is called when a "uid" string is encountered within Object. Its only job
// is to set the uid on the current (top) Level.
func (p *Parser) Uid(n byte) (ParserState, error) {
	if n != '"' {
		return nil, errors.New(fmt.Sprintf("expected uid string, instead found: %c\n", n))
	}
	p.Levels.FoundSubject(p.String())
	return p.Object, nil
}

// openValueLevel is used by Value when a non-scalar value is found.
func (p *Parser) openValueLevel(closing byte, array bool, next ParserState) ParserState {
	// peek the next node to see if it's an empty object or array
	if byte(p.Parsed.Tape[p.Cursor+1]>>56) == closing {
		// it is an empty {} or [], so skip past it
		p.Cursor++
		p.Iter.AdvanceInto()
		// we always return to Object even if array = true because it's the
		// default state where most of the work gets done
		return p.Object
	}
	// add a new level to the stack
	l := p.Levels.Deeper(array)
	// the current quad is waiting until the object is done being parsed because
	// we have to wait until we find/generate a uid
	l.Wait = p.Quad
	p.Quad = NewQuad()
	// either return to Object or Array, depending on the type
	return next
}

// getScalarValue is used by Value and Array
func (p *Parser) getScalarValue(n byte) {
	switch n {
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
}

func (p *Parser) getFacet(n byte) error {
	var err error
	var val interface{}
	switch n {
	case '"':
		s := p.String()
		t, err := types.ParseTime(s)
		if err == nil {
			p.Facet.ValType = api.Facet_DATETIME
			val = t
		} else {
			if p.Facet, err = facets.FacetFor(p.Facet.Key, strconv.Quote(s)); err != nil {
				return err
			}
			return nil
		}
	case 'l', 'u', 'd', 't', 'f', 'n':
		val = p.getFacetValue(n)
	}
	if p.Facet, err = facets.ToBinary(p.Facet.Key, val, p.Facet.ValType); err != nil {
		return err
	}
	return nil
}

func (p *Parser) getFacetValue(n byte) interface{} {
	var val interface{}
	switch n {
	case 'u':
		// NOTE: dgraph doesn't have uint64 facet type, so we just convert it to
		//       int64
		fallthrough
	case 'l':
		p.Facet.ValType = api.Facet_INT
		p.Cursor++
		val = int64(p.Parsed.Tape[p.Cursor])
	case 'd':
		p.Facet.ValType = api.Facet_FLOAT
		p.Cursor++
		val = math.Float64frombits(p.Parsed.Tape[p.Cursor])
	case 't':
		p.Facet.ValType = api.Facet_BOOL
		val = true
	case 'f':
		p.Facet.ValType = api.Facet_BOOL
		val = false
	// TODO: can facets have null values?
	case 'n':
	}
	return val
}

// TODO: allow "type" definition to be anywhere in the object, not just first
func (p *Parser) isGeo() bool {
	if uint64(len(p.Parsed.Tape))-p.Cursor < 3 {
		return false
	}
	if byte(p.Parsed.Tape[p.Cursor+1]>>56) != '"' {
		return false
	}
	if byte(p.Parsed.Tape[p.Cursor+3]>>56) != '"' {
		return false
	}
	totalStringSize := uint64(0)
	p.Cursor++
	maybeGeoType := p.String()
	totalStringSize += uint64(len(maybeGeoType))
	if maybeGeoType != "type" {
		p.Cursor -= 2
		p.StringCursor -= uint64(len(maybeGeoType))
		return false
	}
	p.Cursor++
	maybeGeoType = p.String()
	totalStringSize += uint64(len(maybeGeoType))
	switch maybeGeoType {
	case "Point", "MultiPoint":
	case "LineString", "MultiLineString":
	case "Polygon", "MultiPolygon":
	case "GeometryCollection":
	default:
		p.Cursor -= 2
		p.StringCursor -= uint64(len(maybeGeoType))
		return false
	}
	p.Cursor -= 4
	p.StringCursor -= totalStringSize
	return true
}

func (p *Parser) getGeoValue() error {
	// skip over the geo object
	next := uint64(((p.Parsed.Tape[p.Cursor] << 8) >> 8) - 1)
	stringSize := uint64(0)
	for i := p.Cursor; i < next; i++ {
		c := byte(p.Parsed.Tape[i] >> 56)
		if c == '"' {
			stringSize += p.Parsed.Tape[i+1]
		}
		if c == '"' || c == 'l' || c == 'u' || c == 'd' {
			i++
		}
	}
	// adjust both cursors to the end of this object
	p.StringCursor += stringSize
	p.Cursor = next
	// get an iterator only containing the geo object
	var geoIter json.Iter
	if _, err := p.Iter.AdvanceIter(&geoIter); err != nil {
		return err
	}
	// convert the geo object into json bytes
	object, err := geoIter.MarshalJSON()
	if err != nil {
		return err
	}
	var geoStruct geom.T
	if err = geojson.Unmarshal(object, &geoStruct); err != nil {
		return err
	}
	var geoVal *api.Value
	if geoVal, err = types.ObjectValue(types.GeoID, geoStruct); err != nil {
		return err
	}
	// TODO: move everything over to *api.NQuad so we can use this *api.Value
	_ = geoVal
	return nil
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

func (p *ParserLevels) FoundScalarFacet(predicate string, facet *api.Facet) bool {
	for i := len(p.Levels) - 1; i >= 0; i-- {
		if p.Levels[i].Wait != nil && p.Levels[i].Wait.Predicate == predicate {
			p.Levels[i].Wait.Facets = append(p.Levels[i].Wait.Facets, facet)
			return true
		}
	}
	return false
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
