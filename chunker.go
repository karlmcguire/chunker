package chunker

import (
	"fmt"

	json "github.com/minio/simdjson-go"
)

type Parser struct {
	Parsed *json.ParsedJson
	Quads  []*Quad

	stringOffset uint64
}

func NewParser() *Parser {
	return &Parser{
		Quads: make([]*Quad, 0),
	}
}

func (p *Parser) GetString(l uint64) string {
	s := string(p.Parsed.Strings[p.stringOffset : p.stringOffset+l])
	p.stringOffset += l
	return s
}

func (p *Parser) Walk() (err error) {
	for i := 0; ; i++ {
		node := p.Parsed.Tape[i]
		switch byte(node >> 56) {
		case 'r':
		case 'n':
		case 't':
		case 'f':
		case 'l':
		case 'u':
		case 'd':
		case '"':
			fmt.Println(p.GetString(p.Parsed.Tape[i+1]))
		case '[':
		case ']':
		case '{':
		case '}':
			fmt.Printf("%v\n", uint64(node<<8)>>8)
			if uint64(node<<8)>>8 == 1 {
				return
			}
		}
	}
	return
}

func (p *Parser) Parse(d []byte) ([]*Quad, error) {
	var err error
	if p.Parsed, err = json.Parse(d, nil); err != nil {
		return nil, err
	}
	return p.Quads, p.Walk()
}

type Quad struct {
	Subject   string
	Predicate string
	ObjectId  string
	ObjectVal interface{}
}
