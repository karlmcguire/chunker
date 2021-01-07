package chunker

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	"github.com/dgraph-io/dgraph/tok"
	"github.com/dgraph-io/dgraph/types"
	json "github.com/minio/simdjson-go"
)

type FacetType uint8

const (
	STRING FacetType = iota
	INT
	FLOAT
	BOOL
	DATETIME
)

func (t FacetType) String() string {
	switch t {
	case STRING:
		return "STRING"
	case INT:
		return "INT"
	case FLOAT:
		return "FLOAT"
	case BOOL:
		return "BOOL"
	case DATETIME:
		return "DATETIME"
	}
	return "?"
}

type Facet struct {
	Pred    string
	Key     string
	Value   []byte
	ValType FacetType
	Tokens  []string
	//Alias   string
}

type Quad struct {
	Subject   string
	Predicate string
	ObjectId  string
	ObjectVal interface{}
	Facets    []*Facet
}

func NewQuad() *Quad {
	return &Quad{
		Facets: make([]*Facet, 0),
	}
}

type ParserState uint8

const (
	NONE ParserState = iota
	PREDICATE
	SCALAR
	OBJECT
	ARRAY
	FACET
	FACET_SCALAR
	FACET_MAP
	UID
	GEO
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
	case FACET:
		return "FACET"
	case FACET_SCALAR:
		return "FACET_SCALAR"
	case FACET_MAP:
		return "FACET_MAP"
	case UID:
		return "UID"
	case GEO:
		return "GEO"
	}
	return "?"
}

type (
	Queue struct {
		Waiting []*QueueQuad
	}
	QueueQuad struct {
		Type ParserState
		Quad *Quad
	}
)

func NewQueue() *Queue {
	return &Queue{
		Waiting: make([]*QueueQuad, 0),
	}
}

func (q *Queue) Recent(t ParserState) bool {
	return q.Waiting[len(q.Waiting)-1].Type == t
}

func (q *Queue) Pop(t ParserState) *Quad {
	waiting := q.Waiting[len(q.Waiting)-1]
	if waiting.Type != t {
		return nil
	}
	q.Waiting = q.Waiting[:len(q.Waiting)-1]
	return waiting.Quad
}

func (q *Queue) Add(t ParserState, quad *Quad) {
	q.Waiting = append(q.Waiting, &QueueQuad{
		Type: t,
		Quad: quad,
	})
}

func (q *Queue) Empty() bool {
	return len(q.Waiting) == 0
}

type (
	Depth struct {
		Counter uint64
		Levels  []*DepthLevel
	}
	DepthLevel struct {
		Type   ParserState
		Uids   []string
		Uid    string
		Closes uint64
	}
)

func NewDepthLevel(t ParserState, counter, closes uint64) *DepthLevel {
	return &DepthLevel{
		Type:   t,
		Uids:   make([]string, 0),
		Uid:    fmt.Sprintf("c.%d", counter),
		Closes: closes,
	}
}

func (l *DepthLevel) Subject() string {
	if len(l.Uids) == 0 {
		return l.Uid
	}
	return l.Uids[len(l.Uids)-1]
}

func NewDepth() *Depth {
	return &Depth{
		Levels: make([]*DepthLevel, 0),
	}
}

func (d *Depth) Closes() uint64 {
	if len(d.Levels) < 1 {
		return 0
	}
	return d.Levels[len(d.Levels)-1].Closes
}

func (d *Depth) ArrayObject() bool {
	if len(d.Levels) < 2 {
		return false
	}
	return d.Levels[len(d.Levels)-2].Type == ARRAY
}

func (d *Depth) ArrayUid(uid string) {
	if len(d.Levels) < 2 {
		return
	}
	array := d.Levels[len(d.Levels)-2]
	array.Uids = append(array.Uids, uid)
}

func (d *Depth) Uid(uid string) {
	curr := d.Levels[len(d.Levels)-1]
	curr.Uids = append(curr.Uids, uid)
}

func (d *Depth) Subject() string {
	return d.Levels[len(d.Levels)-1].Subject()
}

func (d *Depth) Increase(t ParserState, closes uint64) {
	if t == OBJECT {
		d.Counter++
	}
	d.Levels = append(d.Levels, NewDepthLevel(t, d.Counter, closes))
}

func (d *Depth) Decrease(t ParserState) *DepthLevel {
	top := d.Levels[len(d.Levels)-1]
	d.Levels = d.Levels[:len(d.Levels)-1]
	return top
}

type Parser struct {
	State  ParserState
	Parsed *json.ParsedJson
	Quads  []*Quad
	Facets []*Facet
	Queue  *Queue
	Depth  *Depth
	Quad   *Quad
	Facet  *Facet
	Skip   bool
	Logs   bool

	stringOffset uint64
}

func NewParser(logs bool) *Parser {
	return &Parser{
		State:  NONE,
		Quads:  make([]*Quad, 0),
		Facets: make([]*Facet, 0),
		Quad:   &Quad{},
		Facet:  &Facet{},
		Queue:  NewQueue(),
		Depth:  NewDepth(),
		Logs:   logs,
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

func (p *Parser) Log(i uint64, c uint64, n byte) {
	if p.Logs {
		switch byte(c >> 56) {
		case 'r', 'n', 't', 'f', 'l', 'u', 'd', '"', '[', ']', '{', '}':
			fmt.Printf("%2d: %c %c %s\n", i, byte(c>>56), n, p.State)
		default:
		}
	}
}

func (p *Parser) LogMore(s string) {
	if p.Logs {
		fmt.Printf("\n        %s\n\n", s)
	}
}

func (p *Parser) Walk() (err error) {
	// n is a placeholder for the next node
	n := byte('n')

	for i := uint64(0); i < uint64(len(p.Parsed.Tape))-1; i++ {
		// c is the current node on the tape
		c := p.Parsed.Tape[i]

		// skip over things like {} and []
		if p.Skip {
			p.Log(i, c, 0)
			p.Skip = false
			continue
		}

		// switch over the current node type
		switch byte(c >> 56) {

		// string
		case '"':
			// p.String grabs the string value from the string buffer and
			// increments p.stringOffset to account for the length
			s := p.String(p.Parsed.Tape[i+1])

			// n is the next node type
			n = byte(p.Parsed.Tape[i+2] >> 56)

			switch p.State {
			case PREDICATE:
				p.FoundPredicate(s)

				// check if facet
				var keys []string
				if strings.Contains(s, "|") {
					keys = strings.Split(s, "|")
					if len(keys) == 2 {
						p.State = FACET
					}
				}

				switch n {
				case '{':
					if p.State == FACET {
						p.LogMore("found facet object")
					}
					p.State = OBJECT
					p.FoundSubject(OBJECT, p.Depth.Subject())
				case '[':
					if p.State == FACET {
						p.LogMore("found facet array")
					}
					p.State = ARRAY
					p.FoundSubject(ARRAY, p.Depth.Subject())
				default:
					switch p.Quad.Predicate {
					case "uid":
						p.State = UID
					case "type":
						p.State = GEO
					default:
						if p.State == FACET {
							p.State = FACET_SCALAR
							p.Quad = NewQuad()
							p.State = FACET_SCALAR
							p.Facet.Pred = keys[0]
							p.Facet.Key = keys[1]
						} else {
							p.State = SCALAR
						}
					}
				}

			case SCALAR:
				p.State = PREDICATE
				p.FoundValue(s)

			case UID:
				p.State = PREDICATE
				p.FoundUid(s)

			case GEO:
				switch s {
				case "Point", "MultiPoint":
					fallthrough
				case "LineString", "MultiLineString":
					fallthrough
				case "Polygon", "MultiPolygon":
					fallthrough
				case "GeometryCollection":
					// TODO: parsing geojson is hard so right now we skip over
					//       the object
					i = p.Depth.Closes()
					p.LogMore(fmt.Sprintf("skipping %s geo object", s))
					p.State = PREDICATE
				}

			case FACET_SCALAR:
				p.State = PREDICATE
				err = p.FoundScalarFacet(s)
				if err != nil {
					return err
				}
			}

		// array open
		case '[':
			n = byte(p.Parsed.Tape[i+1] >> 56)
			if n != ']' {
				p.Depth.Increase(ARRAY, (c<<8)>>8-1)
			}

			p.LogMore(fmt.Sprintf("closing [ at %d", (c<<8)>>8-1))

			switch n {
			case '[':
				p.State = ARRAY
			case ']':
				p.Queue.Pop(ARRAY)
				p.State = PREDICATE
				p.Skip = true
			case '{':
				p.State = OBJECT
			default:
				p.State = SCALAR
			}

		// array close
		case ']':
			n = byte(p.Parsed.Tape[i+1] >> 56)

			if !p.Queue.Empty() {
				if waiting := p.Queue.Pop(ARRAY); waiting != nil {
					uids := p.Depth.Decrease(ARRAY).Uids
					for _, uid := range uids {
						p.Quads = append(p.Quads, &Quad{
							Subject:   p.Depth.Subject(),
							Predicate: waiting.Predicate,
							ObjectId:  uid,
							// we need this incase we find a facet for this
							// predicate later in the file, then we can append
							Facets: make([]*Facet, 0),
						})
					}
				}
			}

			switch n {
			case '[':
				p.State = ARRAY
			case '{':
				p.State = OBJECT
			case '"', '}':
				p.State = PREDICATE
			}

		// object open
		case '{':
			n = byte(p.Parsed.Tape[i+1] >> 56)
			if n != '}' {
				p.Depth.Increase(OBJECT, (c<<8)>>8-1)
			}

			p.LogMore(fmt.Sprintf("closing { at %d", (c<<8)>>8-1))

			switch n {
			case '{':
				p.State = OBJECT
			case '}':
				p.State = PREDICATE
				p.Queue.Pop(OBJECT)
				p.Skip = true
			case '[':
				p.State = ARRAY
			case '"':
				p.State = PREDICATE
			}

		// object close
		case '}':
			n = byte(p.Parsed.Tape[i+1] >> 56)
			if p.Depth.ArrayObject() {
				p.Depth.ArrayUid(p.Depth.Subject())
			}
			objectId := p.Depth.Decrease(OBJECT).Subject()
			if !p.Queue.Empty() {
				if waiting := p.Queue.Pop(OBJECT); waiting != nil {
					p.Quads = append(p.Quads, &Quad{
						Subject:   p.Depth.Subject(),
						Predicate: waiting.Predicate,
						ObjectId:  objectId,
						// we need this incase we find a facet for this
						// predicate later in the file, then we can append
						Facets: make([]*Facet, 0),
					})
				}
			}

			switch n {
			case '{':
				p.State = OBJECT
			case '"', '}', ']':
				p.State = PREDICATE
			}

		// root
		case 'r':
			n = byte(p.Parsed.Tape[i+1] >> 56)

			switch n {
			case '{':
				p.State = OBJECT
			case '[':
				p.State = ARRAY
			}

		// null
		case 'n':
			n = byte(p.Parsed.Tape[i+1] >> 56)

		// true
		case 't':
			n = byte(p.Parsed.Tape[i+1] >> 56)

			switch p.State {
			case SCALAR:
				p.State = PREDICATE
				p.FoundValue(true)
			case FACET_SCALAR:
				p.State = PREDICATE
				err = p.FoundScalarFacet(true)
				if err != nil {
					return err
				}
			}

		// false
		case 'f':
			n = byte(p.Parsed.Tape[i+1] >> 56)

			switch p.State {
			case SCALAR:
				p.State = PREDICATE
				p.FoundValue(false)
			case FACET_SCALAR:
				p.State = PREDICATE
				err = p.FoundScalarFacet(false)
				if err != nil {
					return err
				}
			}

		// int64
		case 'l':
			n = byte(p.Parsed.Tape[i+2] >> 56)

			switch p.State {
			case SCALAR:
				p.State = PREDICATE
				// int64 value is stored after the current node (i + 1)
				p.FoundValue(int64(p.Parsed.Tape[i+1]))
			case FACET_SCALAR:
				p.State = PREDICATE
				err = p.FoundScalarFacet(p.Parsed.Tape[i+1])
				if err != nil {
					return err
				}
			}

		// uint64
		case 'u':
			n = byte(p.Parsed.Tape[i+2] >> 56)

			switch p.State {
			case SCALAR:
				p.State = PREDICATE
				// uint64 value is stored after the current node (i + 1)
				p.FoundValue(p.Parsed.Tape[i+1])
			case FACET_SCALAR:
				p.State = PREDICATE
				err = p.FoundScalarFacet(p.Parsed.Tape[i+1])
				if err != nil {
					return err
				}
			}

		// float64
		case 'd':
			n = byte(p.Parsed.Tape[i+2] >> 56)

			switch p.State {
			case SCALAR:
				p.State = PREDICATE
				p.LogMore(fmt.Sprintf("found float %d", p.Parsed.Tape[i+1]))
				// float64 value is stored after the current node (i + 1)
				p.FoundValue(math.Float64frombits(p.Parsed.Tape[i+1]))
			case FACET_SCALAR:
				p.State = PREDICATE
				err = p.FoundScalarFacet(math.Float64frombits(p.Parsed.Tape[i+1]))
				if err != nil {
					return err
				}
			}
		}

		p.Log(i, c, n)
	}
	return
}

func (p *Parser) FoundUid(s string) {
	p.Depth.Uid(s)
	p.Quad = NewQuad()
}

func (p *Parser) FoundSubject(t ParserState, s string) {
	p.Queue.Add(t, p.Quad)
	p.Quad = NewQuad()
}

func (p *Parser) FoundPredicate(s string) {
	p.Quad.Predicate = s
}

func (p *Parser) FoundValue(v interface{}) {
	p.Quad.Subject = p.Depth.Subject()
	p.Quad.ObjectVal = v
	p.Quads = append(p.Quads, p.Quad)
	p.Quad = NewQuad()
}

func (p *Parser) FoundScalarFacet(v interface{}) error {
	switch val := v.(type) {
	case string:
		if t, err := types.ParseTime(val); err == nil {
			p.Facet.ValType = DATETIME
			b, err := t.MarshalBinary()
			if err != nil {
				return err
			}
			p.Facet.Value = b
		} else {
			p.Facet.ValType = STRING
			p.Facet.Value = []byte(val)
			t, err := tok.GetTermTokens([]string{val})
			if err != nil {
				return err
			}
			p.Facet.Tokens = t
		}
	case int64:
		p.Facet.ValType = INT
		p.Facet.Value = []byte{
			byte(0xff & val),
			byte(0xff & (val >> 8)),
			byte(0xff & (val >> 16)),
			byte(0xff & (val >> 24)),
			byte(0xff & (val >> 32)),
			byte(0xff & (val >> 40)),
			byte(0xff & (val >> 48)),
			byte(0xff & (val >> 56))}
	case uint64:
		p.Facet.ValType = INT
		p.Facet.Value = make([]byte, 8)
		binary.LittleEndian.PutUint64(p.Facet.Value, val)
	case float64:
		p.Facet.ValType = FLOAT
		n := math.Float64bits(val)
		p.Facet.Value = []byte{
			byte(0xff & n),
			byte(0xff & (n >> 8)),
			byte(0xff & (n >> 16)),
			byte(0xff & (n >> 24)),
			byte(0xff & (n >> 32)),
			byte(0xff & (n >> 40)),
			byte(0xff & (n >> 48)),
			byte(0xff & (n >> 56))}
	case bool:
		p.Facet.ValType = BOOL
		if val {
			p.Facet.Value = []byte{0x01}
		} else {
			p.Facet.Value = []byte{0x00}
		}
	}
	// search quads in reverse order for the facet predicate
	for i := len(p.Quads) - 1; i > 0; i-- {
		if p.Quads[i].Predicate == p.Facet.Pred {
			p.Quads[i].Facets = append(p.Quads[i].Facets, p.Facet)
			p.Facet = &Facet{}
			break
		}
	}
	return nil
}
