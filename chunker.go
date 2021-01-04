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

type Parser struct {
	State  ParserState
	Parsed *json.ParsedJson
	Quads  []*Quad
	Quad   *Quad

	stringOffset uint64
}

func NewParser() *Parser {
	return &Parser{
		Quads: make([]*Quad, 0),
		Quad:  &Quad{},
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
	/*
		switch byte(c >> 56) {
		case 'r', 'n', 't', 'f', 'l', 'u', 'd', '"', '[', ']', '{', '}':
			fmt.Printf("%2d: %c", i, c>>56)
		default:
		}
	*/
}

func (p *Parser) LogNext(c byte) {
	//fmt.Printf(" %c %s\n", c, p.State)
}

func (p *Parser) Walk() (err error) {
	for i := 0; i < len(p.Parsed.Tape)-1; i++ {
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
				p.Quad.Predicate = s
				switch n {
				case '{':
					p.State = OBJECT
				case '[':
					p.State = ARRAY
				case '"', 't', 'f', 'l', 'd', 'u', 'n':
					p.State = SCALAR
				}
			case SCALAR:
				p.Quad.ObjectVal = s
				p.Quads = append(p.Quads, p.Quad)
				p.Quad = &Quad{}
				switch n {
				case '{':
					p.State = OBJECT
				case '[':
					p.State = ARRAY
				case '"':
					p.State = PREDICATE
				}
			}

			p.LogNext(n)

		// array open
		case '[':
			n := byte(p.Parsed.Tape[i+1] >> 56)

			switch n {
			case '[':
			case '{':
				p.State = OBJECT
			case '"':
				p.State = PREDICATE
			case 't', 'f', 'l', 'u', 'd':
				p.State = SCALAR
			}

			p.LogNext(n)

		// array close
		case ']':
			n := byte(p.Parsed.Tape[i+1] >> 56)
			p.LogNext(n)

		// object open
		case '{':
			p.State = PREDICATE
			n := byte(p.Parsed.Tape[i+1] >> 56)
			p.LogNext(n)

		// object close
		case '}':
			p.State = PREDICATE
			n := byte(p.Parsed.Tape[i+1] >> 56)
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

			p.State = PREDICATE

			p.LogNext(n)

		// false
		case 'f':
			n := byte(p.Parsed.Tape[i+1] >> 56)

			p.State = PREDICATE

			p.LogNext(n)

		// int64
		case 'l':
			n := byte(p.Parsed.Tape[i+1] >> 56)

			p.State = PREDICATE

			p.LogNext(n)

		// uint64
		case 'u':
			n := byte(p.Parsed.Tape[i+1] >> 56)

			p.State = PREDICATE

			p.LogNext(n)

		// float64
		case 'd':
			n := byte(p.Parsed.Tape[i+1] >> 56)

			p.State = PREDICATE

			p.LogNext(n)
		}
	}
	return
}
