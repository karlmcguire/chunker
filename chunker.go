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
	ARRAY_SCALAR
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
	case ARRAY_SCALAR:
		return "ARRAY_SCALAR"
	}
	return "?"
}

type (
	Depth struct {
		Levels []*Level
	}

	Level struct {
		Type ParserState
		Uids []string
	}
)

func NewDepth() *Depth {
	return &Depth{
		Levels: make([]*Level, 0),
	}
}

func (d *Depth) Increase(t ParserState) {
	d.Levels = append(d.Levels, &Level{
		Type: t,
		Uids: make([]string, 0),
	})
}

func (d *Depth) Decrease(t ParserState) {
	d.Levels = d.Levels[:len(d.Levels)-1]
}

type Parser struct {
	State ParserState
	Quads []*Quad
	Depth *Depth
	Quad  *Quad
	Skip  bool
}

func NewParser() *Parser {
	return &Parser{
		State: NONE,
		Quads: make([]*Quad, 0),
		Depth: NewDepth(),
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
// NOTE: only mutate p.State from within this function
func (p *Parser) Scan(c, n json.Tag, i json.Iter) (done bool, err error) {
	if p.Skip {
		p.Skip = false
		return
	}

	defer p.Log(c, n)
	switch c {

	case json.TagString:
		switch p.State {
		case PREDICATE:
			if err = p.FoundPredicate(i.String()); err != nil {
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

	case json.TagFloat:
		switch p.State {
		case SCALAR:
			if p.State, err = p.FoundValue(i.Float()); err != nil {
				return
			}
		}

	case json.TagUint, json.TagInteger:
		switch p.State {
		case SCALAR:
			if p.State, err = p.FoundValue(i.Int()); err != nil {
				return
			}
		}

	case json.TagBoolFalse, json.TagBoolTrue:
		switch p.State {
		case SCALAR:
			if p.State, err = p.FoundValue(i.Bool()); err != nil {
				return
			}
		}

	case json.TagObjectStart:
		if n != json.TagObjectEnd {
			p.Depth.Increase(OBJECT)
		}
		switch n {
		case json.TagString:
			p.State = PREDICATE
		case json.TagObjectStart:
			p.State = OBJECT
		case json.TagObjectEnd:
			p.State = PREDICATE
			p.Skip = true
		case json.TagArrayStart:
			p.State = ARRAY
		}

	case json.TagObjectEnd:
		p.Depth.Decrease(OBJECT)
		switch n {
		case json.TagString:
			fallthrough
		case json.TagObjectEnd:
			fallthrough
		case json.TagArrayEnd:
			p.State = PREDICATE
		case json.TagObjectStart:
			p.State = OBJECT
		}

	case json.TagArrayStart:
		if n != json.TagArrayEnd {
			p.Depth.Increase(ARRAY)
		}
		switch n {
		case json.TagString:
			fallthrough
		case json.TagFloat:
			fallthrough
		case json.TagUint, json.TagInteger:
			fallthrough
		case json.TagBoolFalse, json.TagBoolTrue:
			p.State = ARRAY_SCALAR
		case json.TagObjectStart:
			p.State = OBJECT
		case json.TagArrayStart:
			p.State = ARRAY
		case json.TagArrayEnd:
			p.State = PREDICATE
			p.Skip = true
		}

	case json.TagArrayEnd:
		p.Depth.Decrease(ARRAY)
		switch n {
		case json.TagString:
			fallthrough
		case json.TagObjectEnd:
			fallthrough
		case json.TagArrayEnd:
			p.State = PREDICATE
		case json.TagArrayStart:
			p.State = ARRAY
		case json.TagObjectStart:
			p.State = OBJECT
		}

	case json.TagNull:
	case json.TagRoot:
	case json.TagEnd:
		done = true
	}
	return
}

func (p *Parser) FoundPredicate(s string, err error) error {
	p.Quad.Predicate = s
	return err
}

func (p *Parser) FoundValue(v interface{}, err error) (ParserState, error) {
	return NONE, nil
}

func (p *Parser) Log(c, n json.Tag) {
	fmt.Println(c, n, p.State, p.Depth.Levels)
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
