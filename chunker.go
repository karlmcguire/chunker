package chunker

import (
	"fmt"
	"reflect"
	"runtime"

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
		Quads  []*Quad
		Parsed *json.ParsedJson
	}
)

func NewParser() *Parser {
	return &Parser{
		Quads: make([]*Quad, 0),
	}
}

func (p *Parser) Run(d []byte) (err error) {
	if p.Parsed, err = json.Parse(d, nil); err != nil {
		return
	}
	for state := p.Root; state != nil; {
		p.Log(state)
		if state, err = state(); err != nil {
			return
		}
	}
	return
}

func (p *Parser) Log(state ParserState) {
	name := runtime.FuncForPC(reflect.ValueOf(state).Pointer()).Name()
	fmt.Println(name[len(name)-7 : len(name)-3])
}

////////////////////////////////////////////////////////////////////////////////

func (p *Parser) Root() (ParserState, error) {
	return nil, nil
}
