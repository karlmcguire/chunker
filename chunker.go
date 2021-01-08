package chunker

import (
	"errors"
	"fmt"
	"math"

	"github.com/davecgh/go-spew/spew"
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

type ParserState func() (ParserState, error)

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

type Parser struct {
	Pos       uint64
	StringPos uint64
	Quad      *Quad
	Levels    []*Level
	Quads     []*Quad
	Data      *json.ParsedJson
}

func NewParser() *Parser {
	return &Parser{
		Quad:   &Quad{},
		Quads:  make([]*Quad, 0),
		Levels: make([]*Level, 0),
	}
}

func (p *Parser) Run(d []byte) error {
	parsed, err := json.Parse(d, nil)
	if err != nil {
		return err
	}
	p.Data = parsed
	p.StringPos = 0
	p.Pos = 0
	p.Quads = make([]*Quad, 0)
	p.Quad = &Quad{}
	p.Levels = make([]*Level, 0)
	for state := p.Root; state != nil; {
		c := byte(p.Data.Tape[p.Pos] >> 56)
		switch c {
		case '{', '}', '[', ']', '"', 'l', 'u', 'd', 't', 'f', 'n':
		default:
			c = byte(' ')
		}
		fmt.Printf("%c %s\n", c, spew.Sdump(p.Levels))
		fmt.Println()
		fmt.Println()
		fmt.Println()
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

func (p *Parser) Level(t LevelType, q *Quad) {
	p.Levels = append(p.Levels, &Level{t, q})
}

func (p *Parser) InArray(i int) bool {
	if len(p.Levels) <= i {
		return false
	}
	return p.Levels[len(p.Levels)-1-i].Type == ARRAY
}

func (p *Parser) GetLevel(i int) *Level {
	if len(p.Levels) <= i {
		return nil
	}
	return p.Levels[len(p.Levels)-1-i]
}

func (p *Parser) Root() (ParserState, error) {
	n := p.Data.Tape[p.Next()]

	switch byte(n >> 56) {
	case '{':
		return p.Object, nil
	case '[':
		return p.Array, nil
	}

	return nil, nil
}

func (p *Parser) Object() (ParserState, error) {
	n := p.Data.Tape[p.Next()]

	switch byte(n >> 56) {
	case '}':
		return p.Scan, nil
	case '"':
		p.Quad.Predicate = p.String()
		if p.Quad.Predicate == "uid" {
			p.Quad.Predicate = ""
			return p.Uid, nil
		}
		p.Quad.Subject = getNextUid()
		o := p.GetLevel(0)
		if o != nil {
			o.Quad.Subject = p.Quad.Subject
		}
		return p.Value, nil
	}

	return nil, nil
}

func (p *Parser) Array() (ParserState, error) {
	c := p.Data.Tape[p.Next()]

	switch byte(c >> 56) {
	case '{':
		return p.Object, nil
	case '[':
		return p.Array, nil
	case ']':
		return nil, nil
	case '"':
	case 'l':
	case 'u':
	case 'd':
	case 't':
	case 'f':
	case 'n':
	}

	return nil, nil
}

func (p *Parser) Value() (ParserState, error) {
	c := p.Data.Tape[p.Next()]

	switch byte(c >> 56) {
	case '{':
		p.Level(OBJECT, p.Quad)
		p.Quad = &Quad{}
		return p.ObjectValue, nil
	case '[':
		p.Level(ARRAY, p.Quad)
		p.Quad = &Quad{}
		return p.ArrayValue, nil
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

	p.Quads = append(p.Quads, p.Quad)
	p.Quad = &Quad{Subject: p.Quad.Subject}
	return p.Scan, nil
}

func (p *Parser) ArrayValue() (ParserState, error) {
	c := p.Data.Tape[p.Next()]

	switch byte(c >> 56) {
	case '{':
		p.Level(OBJECT, p.Quad)
		p.Quad = &Quad{}
		return p.Object, nil
	case '}':
	case '[':
	case ']':
		return p.Scan, nil
	case '"':
		return p.ArrayValue, nil
	case 'l':
	case 'u':
	case 'd':
	case 't':
	case 'f':
	case 'n':
	}

	return nil, nil
}

func (p *Parser) ObjectValue() (ParserState, error) {
	c := p.Data.Tape[p.Next()]

	switch byte(c >> 56) {
	case '}':
		return p.Scan, nil
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

func (p *Parser) Uid() (ParserState, error) {
	c := p.Data.Tape[p.Next()]

	switch byte(c >> 56) {
	case '"':
		p.Quad.Subject = p.String()
		o := p.GetLevel(0)
		if o != nil {
			o.Quad.Subject = p.Quad.Subject
		}
		return p.Scan, nil
	default:
		return nil, errors.New("expecting a uid")
	}

	return nil, nil
}

func (p *Parser) Scan() (ParserState, error) {
	c := p.Data.Tape[p.Next()]

	switch byte(c >> 56) {
	case '{':
		return p.Object, nil
	case '}':
		o := p.GetLevel(0)
		if o != nil {
			p.Quads = append(p.Quads, &Quad{
				Subject:   p.Quad.Subject,
				Predicate: p.Quad.Predicate,
				ObjectId:  o.Quad.Subject,
			})
			p.Levels = p.Levels[:len(p.Levels)-1]
		}
		return p.Scan, nil
	case '[':
	case ']':
		return p.Scan, nil
	case '"':
		p.Quad.Predicate = p.String()
		return p.Value, nil
	}

	return nil, nil
}
