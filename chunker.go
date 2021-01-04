package chunker

import (
	"fmt"

	json "github.com/minio/simdjson-go"
)

type Quad struct {
	Subject   string
	Predicate string
	ObjectId  string
	ObjectVal interface{}
}

type ParserState uint8

const (
	NONE ParserState = iota
	PREDICATE
	SCALAR
	OBJECT
	ARRAY
	UID
	GEO
	GEO_COORDS
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
	case UID:
		return "UID"
	case GEO:
		return "GEO"
	case GEO_COORDS:
		return "GEO_COORDS"
	}
	return "?"
}

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

func (q *Queue) Recent(t ParserState) bool {
	return q.Waiting[len(q.Waiting)-1].Type == t
}

func (q *Queue) Pop(t ParserState) *Quad {
	waiting := q.Waiting[len(q.Waiting)-1]
	if waiting.Type != t {
		return nil
	}
	q.Waiting = q.Waiting[:len(q.Waiting)-1]
	return waiting.Quad
}

func (q *Queue) Add(t ParserState, quad *Quad) {
	q.Waiting = append(q.Waiting, &QueueQuad{
		Type: t,
		Quad: quad,
	})
}

func (q *Queue) Empty() bool {
	return len(q.Waiting) == 0
}

type (
	Depth struct {
		Counter uint64
		Levels  []*DepthLevel
	}
	DepthLevel struct {
		Type ParserState
		Uids []string
		Uid  string
	}
)

func NewDepthLevel(t ParserState, c uint64) *DepthLevel {
	return &DepthLevel{
		Type: t,
		Uids: make([]string, 0),
		Uid:  fmt.Sprintf("c.%d", c),
	}
}

func (l *DepthLevel) Subject() string {
	if len(l.Uids) == 0 {
		return l.Uid
	}
	return l.Uids[len(l.Uids)-1]
}

func NewDepth() *Depth {
	return &Depth{
		Levels: make([]*DepthLevel, 0),
	}
}

func (d *Depth) ArrayObject() bool {
	if len(d.Levels) < 2 {
		return false
	}
	return d.Levels[len(d.Levels)-2].Type == ARRAY
}

func (d *Depth) ArrayUid(uid string) {
	if len(d.Levels) < 2 {
		return
	}
	array := d.Levels[len(d.Levels)-2]
	array.Uids = append(array.Uids, uid)
}

func (d *Depth) Uid(uid string) {
	curr := d.Levels[len(d.Levels)-1]
	curr.Uids = append(curr.Uids, uid)
}

func (d *Depth) Subject() string {
	return d.Levels[len(d.Levels)-1].Subject()
}

func (d *Depth) Increase(t ParserState) {
	if t == OBJECT {
		d.Counter++
	}
	d.Levels = append(d.Levels, NewDepthLevel(t, d.Counter))
}

func (d *Depth) Decrease(t ParserState) *DepthLevel {
	top := d.Levels[len(d.Levels)-1]
	d.Levels = d.Levels[:len(d.Levels)-1]
	return top
}

func (d *Depth) String() string {
	o := ""
	for _, level := range d.Levels {
		switch level.Type {
		case OBJECT:
			o += "O "
		case ARRAY:
			o += "A "
		default:
			o += "?"
		}
	}
	return o
}

type Parser struct {
	State  ParserState
	Parsed *json.ParsedJson
	Quads  []*Quad
	Queue  *Queue
	Depth  *Depth
	Quad   *Quad
	Skip   bool

	stringOffset uint64
}

func NewParser() *Parser {
	return &Parser{
		State: NONE,
		Quads: make([]*Quad, 0),
		Quad:  &Quad{},
		Queue: NewQueue(),
		Depth: NewDepth(),
	}
}

func (p *Parser) Parse(d []byte) ([]*Quad, error) {
	var err error
	if p.Parsed, err = json.Parse(d, nil); err != nil {
		return nil, err
	}
	return p.Quads, p.Walk()
}

func (p *Parser) String(l uint64) string {
	s := string(p.Parsed.Strings[p.stringOffset : p.stringOffset+l])
	p.stringOffset += l
	return s
}

func (p *Parser) Log(i int, c uint64) {
	switch byte(c >> 56) {
	case 'r', 'n', 't', 'f', 'l', 'u', 'd', '"', '[', ']', '{', '}':
		fmt.Printf("%2d: %c", i, c>>56)
	default:
	}
}

func (p *Parser) LogNext(c byte) {
	fmt.Printf(" %c %s %s\n", c, p.Depth, p.State)
}

func (p *Parser) Walk() (err error) {
	for i := 0; i < len(p.Parsed.Tape)-1; i++ {
		if p.Skip {
			p.Skip = false
			continue
		}
		// c is the current node on the tape
		c := p.Parsed.Tape[i]
		p.Log(i, c)

		switch byte(c >> 56) {

		// string
		case '"':
			s := p.String(p.Parsed.Tape[i+1])
			n := byte(p.Parsed.Tape[i+2] >> 56)

			switch p.State {
			case PREDICATE:
				p.FoundPredicate(s)

				switch n {
				case '{':
					p.State = OBJECT
					p.FoundSubject(OBJECT, p.Depth.Subject())
				case '[':
					p.State = ARRAY
					p.FoundSubject(ARRAY, p.Depth.Subject())
				default:
					switch p.Quad.Predicate {
					case "uid":
						p.State = UID
					case "type":
						p.State = GEO
					default:
						p.State = SCALAR
					}
				}
			case SCALAR:
				p.State = PREDICATE
				p.FoundValue(s)

			case UID:
				p.State = PREDICATE
				p.FoundUid(s)

			case GEO:
				// TODO:
			}

			p.LogNext(n)

		// array open
		case '[':
			n := byte(p.Parsed.Tape[i+1] >> 56)
			if n != ']' {
				p.Depth.Increase(ARRAY)
			}

			switch n {
			case '[':
				p.State = ARRAY
			case ']':
				p.Queue.Pop(ARRAY)
				p.State = PREDICATE
				p.Skip = true
			case '{':
				p.State = OBJECT
			default:
				p.State = SCALAR
			}

			p.LogNext(n)

		// array close
		case ']':
			n := byte(p.Parsed.Tape[i+1] >> 56)

			if !p.Queue.Empty() {
				if waiting := p.Queue.Pop(ARRAY); waiting != nil {
					uids := p.Depth.Decrease(ARRAY).Uids
					for _, uid := range uids {
						p.Quads = append(p.Quads, &Quad{
							Subject:   p.Depth.Subject(),
							Predicate: waiting.Predicate,
							ObjectId:  uid,
						})
					}
				}
			}

			switch n {
			case '[':
				p.State = ARRAY
			case '{':
				p.State = OBJECT
			case '"', '}':
				p.State = PREDICATE
			}

			p.LogNext(n)

		// object open
		case '{':
			n := byte(p.Parsed.Tape[i+1] >> 56)
			if n != '}' {
				p.Depth.Increase(OBJECT)
			}

			switch n {
			case '{':
				p.State = OBJECT
			case '}':
				p.State = PREDICATE
				p.Queue.Pop(OBJECT)
				p.Skip = true
			case '[':
				p.State = ARRAY
			case '"':
				p.State = PREDICATE
			}

			p.LogNext(n)

		// object close
		case '}':
			n := byte(p.Parsed.Tape[i+1] >> 56)
			if p.Depth.ArrayObject() {
				p.Depth.ArrayUid(p.Depth.Subject())
			}
			objectId := p.Depth.Decrease(OBJECT).Subject()
			if !p.Queue.Empty() {
				if waiting := p.Queue.Pop(OBJECT); waiting != nil {
					p.Quads = append(p.Quads, &Quad{
						Subject:   p.Depth.Subject(),
						Predicate: waiting.Predicate,
						ObjectId:  objectId,
					})
				}
			}

			switch n {
			case '{':
				p.State = OBJECT
			case '"', '}', ']':
				p.State = PREDICATE
			}

			p.LogNext(n)

		// root
		case 'r':
			n := byte(p.Parsed.Tape[i+1] >> 56)

			switch n {
			case '{':
				p.State = OBJECT
			case '[':
				p.State = ARRAY
			}

			p.LogNext(n)

		// null
		case 'n':
			n := byte(p.Parsed.Tape[i+1] >> 56)
			p.LogNext(n)

		// true
		case 't':
			n := byte(p.Parsed.Tape[i+1] >> 56)

			switch p.State {
			case SCALAR:
				p.State = PREDICATE
				p.FoundValue(true)
			}

			p.LogNext(n)

		// false
		case 'f':
			n := byte(p.Parsed.Tape[i+1] >> 56)

			switch p.State {
			case SCALAR:
				p.State = PREDICATE
				p.FoundValue(false)
			}

			p.LogNext(n)

		// int64
		case 'l':
			n := byte(p.Parsed.Tape[i+1] >> 56)

			switch p.State {
			case SCALAR:
				p.State = PREDICATE
				p.FoundValue(n)
			}

			p.LogNext(n)

		// uint64
		case 'u':
			n := byte(p.Parsed.Tape[i+1] >> 56)

			switch p.State {
			case SCALAR:
				p.State = PREDICATE
				// TODO: convert from tape
				p.FoundValue(n)
			}

			p.LogNext(n)

		// float64
		case 'd':
			n := byte(p.Parsed.Tape[i+1] >> 56)

			switch p.State {
			case SCALAR:
				p.State = PREDICATE
				// TODO: convert from tape
				p.FoundValue(n)
			}

			p.LogNext(n)
		}
	}
	return
}

func (p *Parser) FoundUid(s string) {
	p.Depth.Uid(s)
	p.Quad = &Quad{}
}

func (p *Parser) FoundSubject(t ParserState, s string) {
	p.Queue.Add(t, p.Quad)
	p.Quad = &Quad{}
}

func (p *Parser) FoundPredicate(s string) {
	p.Quad.Predicate = s
}

func (p *Parser) FoundValue(v interface{}) {
	p.Quad.Subject = p.Depth.Subject()
	p.Quad.ObjectVal = v
	p.Quads = append(p.Quads, p.Quad)
	p.Quad = &Quad{}
}
