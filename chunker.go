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

var nextUid uint64 = 0

func getNextUid() string {
	nextUid++
	return fmt.Sprintf("c.%d", nextUid)
}

////////////////////////////////////////////////////////////////////////////////

type (
	LevelType uint8
	Level     struct {
		Type LevelType
		Uids []string
	}
	Depth struct {
		Levels []*Level
	}
)

func NewDepth() *Depth {
	return &Depth{
		Levels: make([]*Level, 0),
	}
}

func (d *Depth) SetUid(uid string) {
	if len(d.Levels) == 0 {
		return
	}
	d.Levels[len(d.Levels)-1].Uids[0] = uid
	if len(d.Levels) > 1 {
		under := d.Levels[len(d.Levels)-2]
		if under.Type == ARRAY {
			under.Uids = append(under.Uids, uid)
		}
	}
}

func (d *Depth) Uid() string {
	return d.Levels[len(d.Levels)-1].Uids[0]
}

func (d *Depth) Add(t LevelType) {
	uids := []string{""}
	if t == OBJECT {
		uids[0] = getNextUid()
	}
	d.Levels = append(d.Levels, &Level{
		Type: t,
		Uids: uids,
	})
}

func (d *Depth) Pop() *Level {
	level := d.Levels[len(d.Levels)-1]
	if len(d.Levels) > 1 {
		d.Levels = d.Levels[:len(d.Levels)-1]
	}
	return level
}

func (d *Depth) String() string {
	o := ""
	for _, level := range d.Levels {
		o += level.Type.String() + fmt.Sprintf("(%v) ", level.Uids)
	}
	return o
}

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

////////////////////////////////////////////////////////////////////////////////

type (
	ParserState func(byte) (ParserState, error)
	Parser      struct {
		Cursor       uint64
		StringCursor uint64
		Quad         *Quad
		Quads        []*Quad
		Depth        *Depth
		Parsed       *json.ParsedJson
	}
)

func NewParser() *Parser {
	return &Parser{
		Cursor: 1,
		Quad:   &Quad{},
		Quads:  make([]*Quad, 0),
		Depth:  NewDepth(),
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
	name := strings.Split(line, ".")
	fmt.Printf("-> %c - %s\n%v\n\n",
		p.Parsed.Tape[p.Cursor]>>56, name[3][:len(name[3])-3], p.Depth)
}

func (p *Parser) String() string {
	p.Cursor++
	length := p.Parsed.Tape[p.Cursor]
	s := p.Parsed.Strings[p.StringCursor : p.StringCursor+length]
	p.StringCursor += length
	return string(s)
}

////////////////////////////////////////////////////////////////////////////////

func (p *Parser) Root(n byte) (ParserState, error) {
	switch n {
	case '{':
		p.Depth.Add(OBJECT)
		return p.Object, nil
	case '[':
		p.Depth.Add(ARRAY)
		return p.Array, nil
	}
	return nil, nil
}

func (p *Parser) Uid(n byte) (ParserState, error) {
	switch n {
	case '"':
		p.Depth.SetUid(p.String())
		return p.Predicate, nil
	}
	return nil, nil
}

func (p *Parser) Object(n byte) (ParserState, error) {
	switch n {
	case '"':
		s := p.String()
		if s == "uid" {
			return p.Uid, nil
		}
		p.Quad.Predicate = s
		return p.Value, nil
	case '}':
		p.Depth.Pop()
		return p.Predicate, nil
	}
	return nil, nil
}

func (p *Parser) Predicate(n byte) (ParserState, error) {
	switch n {
	case '"':
		p.Quad.Predicate = p.String()
		return p.Value, nil
	case '}':
		p.Depth.Pop()
		return p.Predicate, nil
	case '{':
		p.Depth.Add(OBJECT)
		return p.Object, nil
	}
	return nil, nil
}

func (p *Parser) Array(n byte) (ParserState, error) {
	switch n {
	case '{':
		p.Depth.Add(OBJECT)
		return p.Object, nil
	}
	return nil, nil
}

func (p *Parser) Value(n byte) (ParserState, error) {
	switch n {
	case '{':
		p.Depth.Add(OBJECT)
		return p.Object, nil
	case '[':
		p.Depth.Add(ARRAY)
		return p.Array, nil
	case '"':
		p.Quad.Subject = p.Depth.Uid()
		p.Quad.ObjectVal = p.String()
		p.Quads = append(p.Quads, p.Quad)
		p.Quad = &Quad{}
		return p.Predicate, nil
	}
	return nil, nil
}
