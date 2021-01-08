package chunker

import (
	"fmt"
	"math"

	json "github.com/minio/simdjson-go"
)

type Quad struct {
	Subject   string
	Predicate string
	ObjectId  string
	ObjectVal interface{}
}

var nextUid uint64 = 0

func getNextUid() string {
	nextUid++
	return fmt.Sprintf("c.%d", nextUid)
}

type LevelType uint8

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

type Level struct {
	Type LevelType
	Quad *Quad
}

type Levels struct {
	Levels [][]*Level
}

func NewLevels() *Levels {
	return &Levels{
		Levels: make([][]*Level, 0),
	}
}

type ParserState func() (ParserState, error)

type Parser struct {
	Data      *json.ParsedJson
	Quad      *Quad
	Quads     []*Quad
	Levels    *Levels
	Pos       uint64
	StringPos uint64
}

func NewParser() *Parser {
	return &Parser{
		Quad:   &Quad{},
		Quads:  make([]*Quad, 0),
		Levels: NewLevels(),
	}
}

func (p *Parser) Run(d []byte) error {
	parsed, err := json.Parse(d, nil)
	if err != nil {
		return err
	}
	p.Data = parsed
	for state := p.Root; state != nil; {
		fmt.Println(p.Log())
		if state, err = state(); err != nil {
			return err
		}
	}
	return nil
}

func (p *Parser) Next() uint64 {
	p.Pos++
	return p.Pos
}

func (p *Parser) String() string {
	p.Pos++
	l := p.Data.Tape[p.Pos]
	s := string(p.Data.Strings[p.StringPos : p.StringPos+l])
	p.StringPos += l
	return s
}

func (p *Parser) Log() string {
	o := " "
	for _, level := range p.Levels {
		o += level.Type.String() + " "
	}
	return o[:len(o)-1]
}

func (p *Parser) Deeper(t LevelType, q *Quad) {
	p.Levels = append(p.Levels, &Level{t, q})
}

func (p *Parser) Level() *Level {
	if len(p.Levels) <= 0 {
		return nil
	}
	return p.Levels[len(p.Levels)-1]
}

func (p *Parser) Root() (ParserState, error) {
	n := p.Data.Tape[p.Next()]

	switch byte(n >> 56) {
	case '{':
		p.Deeper(OBJECT, p.Quad)
		return p.Object, nil
	case '[':
		p.Deeper(ARRAY, p.Quad)
		return p.Array, nil
	}

	return nil, nil
}

func (p *Parser) Uid() (ParserState, error) {
	n := p.Data.Tape[p.Next()]

	switch byte(n >> 56) {
	case '"':
		p.Level().Quad.Subject = p.String()
		return p.LookForPredicate, nil
	}

	return nil, nil
}

func (p *Parser) Object() (ParserState, error) {
	n := p.Data.Tape[p.Next()]

	switch byte(n >> 56) {
	case '}':
	case '"':
		p.Quad.Predicate = p.String()
		if p.Quad.Predicate == "uid" {
			p.Quad.Predicate = ""
			return p.Uid, nil
		}
		p.Quad.Subject = getNextUid()
		return p.Value, nil
	}

	return nil, nil
}

func (p *Parser) Array() (ParserState, error) {
	n := p.Data.Tape[p.Next()]

	switch byte(n >> 56) {
	case '{':
		p.Deeper(OBJECT, p.Quad)
		return p.Object, nil
	}

	return nil, nil
}

func (p *Parser) Value() (ParserState, error) {
	n := p.Data.Tape[p.Next()]

	switch byte(n >> 56) {
	case '{':
		p.Deeper(OBJECT, p.Quad)
		p.Quad = &Quad{}
		return p.Object, nil
	case '[':
		p.Deeper(ARRAY, p.Quad)
		p.Quad = &Quad{}
		return p.Array, nil
	case '"':
		p.Quad.ObjectVal = p.String()
		break
	case 'l':
		p.Quad.ObjectVal = int64(p.Data.Tape[p.Next()])
		break
	case 'u':
		p.Quad.ObjectVal = p.Data.Tape[p.Next()]
		break
	case 'd':
		p.Quad.ObjectVal = math.Float64frombits(p.Data.Tape[p.Next()])
		break
	case 't':
		p.Quad.ObjectVal = true
		break
	case 'f':
		p.Quad.ObjectVal = false
		break
	case 'n':
		p.Quad.ObjectVal = nil
		break
	}

	s := p.Quad.Subject
	p.Quads = append(p.Quads, p.Quad)
	p.Quad = &Quad{Subject: s}
	return p.LookForPredicate, nil
}

func (p *Parser) LookForPredicate() (ParserState, error) {
	n := p.Data.Tape[p.Next()]

	switch byte(n >> 56) {
	case '{':
		p.Deeper(OBJECT, p.Quad)
		p.Quad = &Quad{}
		return p.Object, nil
	case '}':
		l := p.Pop()
		if l != nil && l.Quad != nil && l.Quad.ObjectVal == nil {
			p.Quads = append(p.Quads, &Quad{
				Subject:   p.Subject(),
				Predicate: l.Quad.Predicate,
				ObjectId:  l.Quad.Subject,
			})
		}
		return p.LookForPredicate, nil
	case ']':
		// TODO
		return p.LookForPredicate, nil
	case '"':
		p.Quad.Predicate = p.String()
		if p.Quad.Subject == "" {
			p.Quad.Subject = p.Subject()
		}
		if p.Quad.Subject == "" {
			p.Quad.Subject = getNextUid()
		}
		return p.Value, nil
	}

	return nil, nil
}

func (p *Parser) Subject() string {
	if len(p.Levels) <= 0 {
		return ""
	}
	l := p.Level()
	if l.Quad == nil {
		return ""
	}
	return l.Quad.Subject
}

func (p *Parser) Pop() *Level {
	if len(p.Levels) <= 0 {
		return nil
	}
	l := p.Levels[len(p.Levels)-1]
	p.Levels = p.Levels[:len(p.Levels)-1]
	return l
}
