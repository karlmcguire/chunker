package chunker

import (
	"fmt"

	json "github.com/minio/simdjson-go"
)

var uidCounter = 0

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

////////////////////////////////////////////////////////////////////////////////

type (
	Queue struct {
		Waiting []*QueueQuad
	}

	QueueQuad struct {
		Type ParserState
		Quad *Quad
	}
)

func NewQueue() *Queue {
	return &Queue{
		Waiting: make([]*QueueQuad, 0),
	}
}

func (q *Queue) Add(t ParserState, quad *Quad) {
	q.Waiting = append(q.Waiting, &QueueQuad{
		Type: t,
		Quad: quad,
	})
}

////////////////////////////////////////////////////////////////////////////////

type Level struct {
	Type ParserState
	Uids []string
	Uid  string
}

func NewLevel(t ParserState) *Level {
	uidCounter++
	return &Level{
		Type: t,
		Uids: make([]string, 0),
		Uid:  fmt.Sprintf("%d", uidCounter),
	}
}

func (l *Level) Subject() string {
	return l.Uid
}

////////////////////////////////////////////////////////////////////////////////

type Depth struct {
	Levels []*Level
}

func NewDepth() *Depth {
	return &Depth{
		Levels: make([]*Level, 0),
	}
}

func (d *Depth) Subject() string {
	return d.Levels[len(d.Levels)-1].Subject()
}

func (d *Depth) Increase(t ParserState) {
	d.Levels = append(d.Levels, NewLevel(t))
}

func (d *Depth) Decrease(t ParserState) {
	d.Levels = d.Levels[:len(d.Levels)-1]
}

func (d *Depth) String() string {
	o := ""
	for _, level := range d.Levels {
		if level.Type == OBJECT {
			o += "O "
		} else if level.Type == ARRAY {
			o += "A "
		} else {
			o += "? "
		}
	}
	return o
}

////////////////////////////////////////////////////////////////////////////////

type Parser struct {
	State ParserState
	Quads []*Quad
	Queue *Queue
	Depth *Depth
	Quad  *Quad
	Skip  bool
}

func NewParser() *Parser {
	return &Parser{
		State: NONE,
		Quads: make([]*Quad, 0),
		Depth: NewDepth(),
		Queue: NewQueue(),
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
				p.State = OBJECT
				p.FoundSubject(OBJECT, p.Depth.Subject())
			case json.TagArrayStart:
				p.State = ARRAY
				p.FoundSubject(ARRAY, p.Depth.Subject())
			default:
				switch p.Quad.Predicate {
				case "uid":
				case "type":
				default:
					p.State = SCALAR
				}
			}
		case SCALAR:
			p.State = PREDICATE
			if err = p.FoundValue(i.String()); err != nil {
				return
			}
		case ARRAY_SCALAR:
			p.State = ARRAY_SCALAR
			if err = p.FoundValue(i.String()); err != nil {
				return
			}
		}

	case json.TagFloat:
		switch p.State {
		case SCALAR:
			p.State = PREDICATE
			if err = p.FoundValue(i.Float()); err != nil {
				return
			}
		}

	case json.TagUint, json.TagInteger:
		switch p.State {
		case SCALAR:
			p.State = PREDICATE
			if err = p.FoundValue(i.Int()); err != nil {
				return
			}
		}

	case json.TagBoolFalse, json.TagBoolTrue:
		switch p.State {
		case SCALAR:
			p.State = PREDICATE
			if err = p.FoundValue(i.Bool()); err != nil {
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

	case json.TagNull: // TODO
	case json.TagRoot:
		switch n {
		case json.TagObjectStart:
			p.State = OBJECT
		case json.TagArrayStart:
			p.State = ARRAY
		}

	case json.TagEnd:
		done = true
	}
	return
}

func (p *Parser) FoundSubject(t ParserState, s string) {
	p.Queue.Add(t, p.Quad)
	p.Quad = &Quad{}
}

func (p *Parser) FoundPredicate(s string, err error) error {
	p.Quad.Predicate = s
	return err
}

func (p *Parser) FoundValue(v interface{}, err error) error {
	p.Quad.ObjectVal = v
	p.Quad.Subject = p.Depth.Subject()
	p.Quads = append(p.Quads, p.Quad)
	p.Quad = &Quad{}
	return nil
}

func (p *Parser) Log(c, n json.Tag) {
	fmt.Println(c, n, p.Depth, p.State)
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
