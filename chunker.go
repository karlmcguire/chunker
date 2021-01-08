package chunker

import (
	json "github.com/minio/simdjson-go"
)

type Quad struct {
	Subject   string
	Predicate string
	ObjectId  string
	ObjectVal interface{}
}

type ParserState func() (ParserState, error)

type Parser struct {
	Pos       uint64
	StringPos uint64
	Quad      *Quad

	Quads []*Quad

	Data *json.ParsedJson
}

func NewParser() *Parser {
	return &Parser{
		Quad:  &Quad{},
		Quads: make([]*Quad, 0),
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
		p.Quad.Predicate = n.String()
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
		return p.ObjectValue, nil
	case '[':
	case '"':
		p.Quad.ObjectVal = p.String()
		p.Quads = append(p.Quads, p.Quad)
		p.Quad = &Quad{}
		return p.Scan, nil
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

	}
}

func (p *Parser) Scan() (ParserState, error) {
	c := p.Data.Tape[p.Next()]

	switch byte(c >> 56) {
	case '"':
		p.Quad.Predicate = p.String()
		return p.Value, nil
	case '}':
		return nil, nil
	}

	return nil, nil
}
