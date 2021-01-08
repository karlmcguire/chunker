package chunker

import (
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

////////////////////////////////////////////////////////////////////////////////

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
	Uids []string
}

type Levels struct {
	Levels []*Level
}

func NewLevels() *Levels {
	return &Levels{
		Levels: make([]*Level, 0),
	}
}

func (l *Levels) String() string {
	return spew.Sdump(l.Levels)
}

func (l *Levels) Add(t LevelType, q *Quad) {
	level := &Level{
		Type: t,
		Quad: q,
		Uids: make([]string, 0),
	}
	if t == OBJECT && (q == nil || q.Subject == "") {
		level.Uids = append(level.Uids, getNextUid())
	}
	l.Levels = append(l.Levels, level)
}

func (l *Levels) Pop() *Level {
	if len(l.Levels) <= 0 {
		return nil
	}
	level := l.Levels[len(l.Levels)-1]
	l.Levels = l.Levels[:len(l.Levels)-1]
	return level
}

func (l *Levels) Top() *Level {
	if len(l.Levels) <= 0 {
		return nil
	}
	return l.Levels[len(l.Levels)-1]
}

////////////////////////////////////////////////////////////////////////////////

type ParserState func() (ParserState, error)

type Parser struct {
	Data      *json.ParsedJson
	Quad      *Quad
	Quads     []*Quad
	Levels    *Levels
	Pos       uint64
	StringPos uint64
}

func NewParser() *Parser {
	return &Parser{
		Quad:   &Quad{},
		Quads:  make([]*Quad, 0),
		Levels: NewLevels(),
	}
}

func (p *Parser) Run(d []byte) error {
	parsed, err := json.Parse(d, nil)
	if err != nil {
		return err
	}
	p.Data = parsed
	for state := p.Root; state != nil; {
		//fmt.Println(p.Levels)
		if state, err = state(); err != nil {
			return err
		}
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

func (p *Parser) Root() (ParserState, error) {
	n := p.Data.Tape[p.Next()]

	switch byte(n >> 56) {
	case '{':
		p.Levels.Add(OBJECT, nil)
		return p.Object, nil
	case '[':
		p.Levels.Add(ARRAY, nil)
		return p.Array, nil
	}

	return nil, nil
}

func (p *Parser) ObjectUid() (ParserState, error) {
	n := p.Data.Tape[p.Next()]

	switch byte(n >> 56) {
	case '"':
		fmt.Println("found uid", p.String())
		return p.LookForPredicate, nil
	}

	return nil, nil
}

func (p *Parser) Object() (ParserState, error) {
	n := p.Data.Tape[p.Next()]

	switch byte(n >> 56) {
	case '}':
	case '"':
		p.Quad.Predicate = p.String()
		if p.Quad.Predicate == "uid" {
			p.Quad.Predicate = ""
			return p.ObjectUid, nil
		}
		p.Quad.Subject = getNextUid()
		return p.Value, nil
	}

	return nil, nil
}

func (p *Parser) Array() (ParserState, error) {
	n := p.Data.Tape[p.Next()]

	switch byte(n >> 56) {
	case '{':
		p.Levels.Add(OBJECT, p.Quad)
		return p.Object, nil
	}

	return nil, nil
}

func (p *Parser) Value() (ParserState, error) {
	n := p.Data.Tape[p.Next()]

	switch byte(n >> 56) {
	case '{':
		p.Levels.Add(OBJECT, p.Quad)
		p.Quad = &Quad{}
		return p.Object, nil
	case '[':
		p.Levels.Add(ARRAY, p.Quad)
		p.Quad = &Quad{}
		return p.Array, nil
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

	s := p.Quad.Subject
	p.Quads = append(p.Quads, p.Quad)
	p.Quad = &Quad{Subject: s}
	return p.LookForPredicate, nil
}

func (p *Parser) LookForPredicate() (ParserState, error) {
	n := p.Data.Tape[p.Next()]

	switch byte(n >> 56) {
	case '{':
		p.Levels.Add(OBJECT, p.Quad)
		return p.Object, nil
	case '}':
		spew.Dump(p.Levels)
		l := p.Levels.Pop()
		if a := p.Levels.Top(); a != nil && a.Type == ARRAY {
			p.Quads = append(p.Quads, &Quad{
				Subject:   a.Quad.Subject,
				Predicate: a.Quad.Predicate,
				ObjectId:  l.Quad.Subject,
			})
			// TODO:
			//a.Uids = append(a.Uids, l.Quad.Subject)
		}
		return p.LookForPredicate, nil
	case ']':
		l := p.Levels.Pop()
		fmt.Printf("closing array: %d\n", len(p.Levels.Levels))
		spew.Dump(l)
		fmt.Println()
		fmt.Println()
		fmt.Println()
		return p.LookForPredicate, nil
	case '"':
		p.Quad.Predicate = p.String()
		if p.Quad.Subject == "" {
			fmt.Println("predicate")
			spew.Dump(p.Levels)
			fmt.Println()
			fmt.Println()
			p.Quad.Subject = getNextUid()
		}
		return p.Value, nil
	}

	return nil, nil
}
