package chunker

import (
	"errors"
	"fmt"
	"math"
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
		Scalars bool
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
	fmt.Printf("-> %c - %v\n%v\n",
		p.Parsed.Tape[p.Cursor]>>56, name[0], spew.Sdump(p.Levels))
}

func (p *Parser) String() string {
	p.Cursor++
	length := p.Parsed.Tape[p.Cursor]
	s := p.Parsed.Strings[p.StringCursor : p.StringCursor+length]
	p.StringCursor += length
	return string(s)
}

func (p *Parser) Deeper(t LevelType) *Level {
	var subject string
	if t == OBJECT {
		subject = getNextUid()
	}
	level := &Level{
		Type:    t,
		Subject: subject,
	}
	p.Levels = append(p.Levels, level)
	return level
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

////////////////////////////////////////////////////////////////////////////////

func (p *Parser) Root(n byte) (ParserState, error) {
	switch n {
	case '{':
		p.Deeper(OBJECT)
		return p.Object, nil
	case '[':
		p.Deeper(ARRAY)
		return p.Array, nil
	}
	return nil, nil
}

func (p *Parser) Scan(n byte) (ParserState, error) {
	switch n {
	case '{':
		p.Deeper(OBJECT)
		return p.Object, nil
	case '}':
	case '[':
		p.Deeper(ARRAY)
		return p.Array, nil
	case ']':
	case 'l':
	case 'u':
	case 'd':
	case 't':
	case 'f':
	case 'n':
	}
	return nil, nil
}

func (p *Parser) Object(n byte) (ParserState, error) {
	switch n {
	case '{':
		p.Deeper(OBJECT)
		return p.Object, nil
	case '}':
		l := p.Levels[len(p.Levels)-1]
		if l.Wait != nil {
			p.Quad = l.Wait
			p.Quad.ObjectId = l.Subject
			p.Quads = append(p.Quads, p.Quad)
			p.Quad = &Quad{}
		} else {
			if len(p.Levels) >= 2 {
				a := p.Levels[len(p.Levels)-2]
				if a.Type == ARRAY && a.Wait != nil {
					p.Quad.Subject = a.Wait.Subject
					p.Quad.Predicate = a.Wait.Predicate
					p.Quad.ObjectId = l.Subject
					p.Quads = append(p.Quads, p.Quad)
					p.Quad = &Quad{}
				}
			}
		}
		p.Levels = p.Levels[:len(p.Levels)-1]
		return p.Object, nil
	case '"':
		s := p.String()
		if s == "uid" {
			return p.Uid, nil
		}
		p.Quad.Subject = p.Subject()
		p.Quad.Predicate = s
		return p.Value, nil
	}
	return p.Scan, nil
}

func (p *Parser) Array(n byte) (ParserState, error) {
	l := p.Levels[len(p.Levels)-1]
	p.Quad.Subject = l.Wait.Subject
	p.Quad.Predicate = l.Wait.Predicate
	switch n {
	case '{':
		p.Deeper(OBJECT)
		return p.Object, nil
	case '}':
		fmt.Println("ahhhhhhhhhh")
	case '[':
		p.Deeper(ARRAY)
		return p.Array, nil
	case ']':
		spew.Dump(p.Levels)
		return nil, nil
	case '"':
		l.Scalars = true
		p.Quad.ObjectVal = p.String()
	case 'l':
		l.Scalars = true
		p.Cursor++
		p.Quad.ObjectVal = int64(p.Parsed.Tape[p.Cursor])
	case 'u':
		l.Scalars = true
		p.Cursor++
		p.Quad.ObjectVal = p.Parsed.Tape[p.Cursor]
	case 'd':
		l.Scalars = true
		p.Cursor++
		p.Quad.ObjectVal = math.Float64frombits(p.Parsed.Tape[p.Cursor])
	case 't':
		l.Scalars = true
		p.Quad.ObjectVal = true
	case 'f':
		l.Scalars = true
		p.Quad.ObjectVal = false
	case 'n':
		l.Scalars = true
		p.Quad.ObjectVal = nil
	}
	p.Quads = append(p.Quads, p.Quad)
	p.Quad = &Quad{}
	return p.Array, nil
}

func (p *Parser) Value(n byte) (ParserState, error) {
	switch n {
	case '{':
		l := p.Deeper(OBJECT)
		l.Wait = p.Quad
		p.Quad = &Quad{}
		return p.Object, nil
	case '[':
		l := p.Deeper(ARRAY)
		l.Wait = p.Quad
		p.Quad = &Quad{}
		return p.Array, nil
	case '"':
		p.Quad.ObjectVal = p.String()
	case 'l':
		p.Cursor++
		p.Quad.ObjectVal = int64(p.Parsed.Tape[p.Cursor])
	case 'u':
		p.Cursor++
		p.Quad.ObjectVal = p.Parsed.Tape[p.Cursor]
	case 'd':
		p.Cursor++
		p.Quad.ObjectVal = math.Float64frombits(p.Parsed.Tape[p.Cursor])
	case 't':
		p.Quad.ObjectVal = true
	case 'f':
		p.Quad.ObjectVal = false
	case 'n':
		p.Quad.ObjectVal = nil
	}
	p.Quads = append(p.Quads, p.Quad)
	p.Quad = &Quad{}
	return p.Object, nil
}

func (p *Parser) Uid(n byte) (ParserState, error) {
	if n != '"' {
		return nil, errors.New("expected uid, instead found: " + p.String())
	}
	p.Levels[len(p.Levels)-1].Subject = p.String()
	return p.Object, nil
}
