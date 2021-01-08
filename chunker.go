package chunker

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"

	json "github.com/minio/simdjson-go"
)

type Quad struct {
	Subject   string
	Predicate string
	ObjectId  string
	ObjectVal interface{}
}

////////////////////////////////////////////////////////////////////////////////

type (
	ParserState func() (ParserState, error)
	Parser      struct {
		Cursor       uint64
		StringCursor uint64
		Quads        []*Quad
		Parsed       *json.ParsedJson
	}
)

func NewParser() *Parser {
	return &Parser{
		Cursor: 1,
		Quads:  make([]*Quad, 0),
	}
}

func (p *Parser) Run(d []byte) (err error) {
	if p.Parsed, err = json.Parse(d, nil); err != nil {
		return
	}
	for state := p.Root; state != nil; p.Cursor++ {
		p.Log(state)
		if state, err = state(); err != nil {
			return
		}
	}
	return
}

func (p *Parser) Log(state ParserState) {
	line := runtime.FuncForPC(reflect.ValueOf(state).Pointer()).Name()
	name := strings.Split(line, ".")
	fmt.Printf("-> %s\n", name[3][:len(name[3])-3])
}

func (p *Parser) String() string {
	p.Cursor++
	length := p.Parsed.Tape[p.Cursor]
	s := p.Parsed.Strings[p.StringCursor : p.StringCursor+length]
	p.StringCursor += length
	return string(s)
}

////////////////////////////////////////////////////////////////////////////////

func (p *Parser) Root() (ParserState, error) {
	n := p.Parsed.Tape[p.Cursor]

	switch byte(n >> 56) {
	case '{':
		return p.Object, nil
	case '[':
	}

	return nil, nil
}

func (p *Parser) Object() (ParserState, error) {
	n := p.Parsed.Tape[p.Cursor]

	switch byte(n >> 56) {
	case '"':
		fmt.Println(p.String())
	default:
		return nil, nil
	}

	return p.Object, nil
}
