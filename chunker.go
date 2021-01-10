package chunker

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"

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

////////////////////////////////////////////////////////////////////////////////

type (
	LevelType uint8
	Level     struct {
		Type    LevelType
		Subject string
		Wait    *Quad
	}
)

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

type (
	ParserState func(byte) (ParserState, error)
	Parser      struct {
		Cursor       uint64
		StringCursor uint64
		Quad         *Quad
		Quads        []*Quad
		Levels       []*Level
		Parsed       *json.ParsedJson
	}
)

func NewParser() *Parser {
	return &Parser{
		Cursor: 1,
		Quad:   &Quad{},
		Quads:  make([]*Quad, 0),
		Levels: make([]*Level, 0),
	}
}

func (p *Parser) Run(d []byte) (err error) {
	if p.Parsed, err = json.Parse(d, nil); err != nil {
		return
	}
	for state := p.Root; state != nil; p.Cursor++ {
		p.Log(state)
		if state, err = state(byte(p.Parsed.Tape[p.Cursor] >> 56)); err != nil {
			return
		}
	}
	return
}

func (p *Parser) Log(state ParserState) {
	line := runtime.FuncForPC(reflect.ValueOf(state).Pointer()).Name()
	name := strings.Split(strings.Split(line, ".")[3], "-")
	fmt.Printf("-> %c - %s\n%v\n",
		p.Parsed.Tape[p.Cursor]>>56, name[0], spew.Sdump(p.Levels))
}

func (p *Parser) String() string {
	p.Cursor++
	length := p.Parsed.Tape[p.Cursor]
	s := p.Parsed.Strings[p.StringCursor : p.StringCursor+length]
	p.StringCursor += length
	return string(s)
}

func (p *Parser) Deeper(t LevelType) {
	var subject string
	if t == OBJECT {
		subject = getNextUid()
	}
	p.Levels = append(p.Levels, &Level{
		Type:    t,
		Subject: subject,
	})
}

func (p *Parser) Subject() string {
	if len(p.Levels) == 0 {
		return "eeeeeeeeee"
	}
	for i := len(p.Levels) - 1; i >= 0; i-- {
		if p.Levels[i].Type == OBJECT {
			return p.Levels[i].Subject
		}
	}
	return "xxxxxxxxx"
}

func (p *Parser) FoundSubject(s string) {
	p.Levels[len(p.Levels)-1].Subject = s
}

////////////////////////////////////////////////////////////////////////////////

func (p *Parser) Root(n byte) (ParserState, error) {
	switch n {
	case '{':
		return p.Object, nil
	case '[':
		return p.Array, nil
	}
	return nil, nil
}

func (p *Parser) Object(n byte) (ParserState, error) {
	p.Deeper(OBJECT)
	switch n {
	case '"':
		p.Quad.Subject = p.Subject()
		p.Quad.Predicate = p.String()
		return p.Value, nil
	}
	return nil, nil
}

func (p *Parser) ObjectValue(n byte) (ParserState, error) {
	return nil, nil
}

func (p *Parser) Array(n byte) (ParserState, error) {
	p.Deeper(ARRAY)
	return nil, nil
}

func (p *Parser) ArrayScalar(n byte) (ParserState, error) {
	return nil, nil
}

func (p *Parser) ArrayObject(n byte) (ParserState, error) {
	p.Deeper(OBJECT)
	switch n {
	case '"':
		s := p.String()
		if s == "uid" {
			return p.Uid(p.ArrayObject), nil
		}
		spew.Dump(p.Levels)
		fmt.Println(s, len(p.Levels), p.Subject())
	}
	return nil, nil
}

func (p *Parser) ArrayValue(n byte) (ParserState, error) {
	switch n {
	case '{':
		return p.ArrayObject, nil
	case '"':
		p.Quad.Subject = p.Subject()
		p.Quad.Predicate = p.Levels[len(p.Levels)-1].Wait.Predicate
		p.Quad.ObjectVal = p.String()
		p.Quads = append(p.Quads, p.Quad)
		p.Quad = &Quad{}
		return p.ArrayValue, nil
	case ']':
		return p.Scan, nil
	}
	return nil, nil
}

func (p *Parser) Value(n byte) (ParserState, error) {
	switch n {
	case '{':
		p.Levels[len(p.Levels)-1].Wait = p.Quad
		p.Quad = &Quad{}
		return p.ObjectValue, nil
	case '[':
		p.Levels[len(p.Levels)-1].Wait = p.Quad
		p.Quad = &Quad{}
		return p.ArrayValue, nil
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

func (p *Parser) Scan(n byte) (ParserState, error) {
	return nil, nil
}

func (p *Parser) Uid(f ParserState) ParserState {
	return func(n byte) (ParserState, error) {
		p.FoundSubject(p.String())
		return f, nil
	}
}
