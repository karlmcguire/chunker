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

type ParserState func() (ParserState, error)

type Parser struct {
	Pos       uint64
	StringPos uint64
	Quad      *Quad
	Objects   []*Quad
	Arrays    []*Quad

	Quads []*Quad

	Data *json.ParsedJson
}

func NewParser() *Parser {
	return &Parser{
		Quad:    &Quad{},
		Objects: make([]*Quad, 0),
		Arrays:  make([]*Quad, 0),
		Quads:   make([]*Quad, 0),
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
	p.Objects = make([]*Quad, 0)
	p.Arrays = make([]*Quad, 0)

	for state := p.Root; state != nil; {
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
	case '"':
		p.Quad.Subject = getNextUid()
		p.Quad.Predicate = p.String()
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
	case ']':
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
		p.Objects = append(p.Objects, p.Quad)
		p.Quad = &Quad{}
		return p.ObjectValue, nil
	case '[':
		p.Arrays = append(p.Arrays, p.Quad)
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
	case '[':
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

func (p *Parser) ObjectValue() (ParserState, error) {
	c := p.Data.Tape[p.Next()]

	switch byte(c >> 56) {
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
		return p.Scan, nil
	}

	return nil, nil
}

func (p *Parser) Scan() (ParserState, error) {
	c := p.Data.Tape[p.Next()]

	switch byte(c >> 56) {
	case '"':
		p.Quad.Predicate = p.String()
		return p.Value, nil
	case '}':
		if len(p.Objects) > 0 {
			objectId := p.Quad.Subject
			p.Quad, p.Objects = p.Objects[len(p.Objects)-1], p.Objects[:len(p.Objects)-1]
			p.Quad.ObjectId = objectId
			p.Quads = append(p.Quads, p.Quad)
			p.Quad = &Quad{}
		}
		return nil, nil
	}

	return nil, nil
}
