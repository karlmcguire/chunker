package chunker

import (
	"fmt"

	json "github.com/minio/simdjson-go"
)

type ParserState uint8

const (
	NONE ParserState = iota
	PREDICATE
	SCALAR
	OBJECT
	ARRAY
)

func (s ParserState) String() string {
	switch s {
	case NONE:
		return "NONE"
	case PREDICATE:
		return "PREDICATE"
	case SCALAR:
		return "SCALAR"
	case OBJECT:
		return "OBJECT"
	case ARRAY:
		return "ARRAY"
	}
	return "?"
}

type Parser struct {
	State ParserState
	Quads []*Quad
	Quad  *Quad
}

func NewParser() *Parser {
	return &Parser{
		State: NONE,
		Quads: make([]*Quad, 0),
		Quad:  &Quad{},
	}
}

// Parse reads from the iterator until an error is raised or we reach the end of
// the tape, returning Quads.
func (p *Parser) Parse(iter json.Iter) ([]*Quad, error) {
	var err error
	for done := false; !done; {
		done, err = p.Scan(iter.AdvanceInto(), iter.PeekNextTag(), iter)
		if err != nil {
			return nil, err
		}
	}
	return p.Quads, nil
}

// Scan is called with the current (c) and next (n) simdjson.Tag on the tape.
// The Parser will continue reading from the tape and calling Scan until it
// returns true or an error.
//
// NOTE: we should only mutate p.State from within this function
func (p *Parser) Scan(c, n json.Tag, i json.Iter) (done bool, err error) {
	defer p.Log(c, n)
	switch c {

	case json.TagString:
		switch p.State {
		case PREDICATE:
			p.Quad.Predicate, err = i.String()
			if err != nil {
				return
			}
			switch n {
			case json.TagObjectStart:
			case json.TagArrayStart:
			default:
			}
		case SCALAR:
		case OBJECT:
		case ARRAY:
		}

	case json.TagInteger:
		if p.State == SCALAR {
			if p.State, err = p.Push(i.Int()); err != nil {
				return
			}
		}

	case json.TagUint:
		if p.State == SCALAR {
			if p.State, err = p.Push(i.Int()); err != nil {
				return
			}
		}

	case json.TagFloat:
		if p.State == SCALAR {
			if p.State, err = p.Push(i.Float()); err != nil {
				return
			}
		}

	case json.TagBoolFalse:
		fallthrough
	case json.TagBoolTrue:
		if p.State == SCALAR {
			if p.State, err = p.Push(i.Bool()); err != nil {
				return
			}
		}

	case json.TagObjectStart:
	case json.TagObjectEnd:
	case json.TagArrayStart:
	case json.TagArrayEnd:
	case json.TagNull:
	case json.TagRoot:
	case json.TagEnd:
		done = true
	}
	return
}

// TODO
func (p *Parser) Push(v interface{}, err error) (ParserState, error) {
	return NONE, nil
}

func (p *Parser) Log(c, n json.Tag) {
	fmt.Println(c, n, p.State)
}

func Parse(d []byte) ([]*Quad, error) {
	tape, err := json.Parse(d, nil)
	if err != nil {
		return nil, err
	}
	return NewParser().Parse(tape.Iter())
}

type Quad struct {
	Subject   string
	Predicate string
	ObjectId  string
	ObjectVal interface{}
}
