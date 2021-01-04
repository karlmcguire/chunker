package chunker

import (
	"fmt"

	json "github.com/minio/simdjson-go"
)

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

type Geo struct {
	Type        string
	Coordinates []float64
}

func NewGeo() *Geo {
	return &Geo{
		Coordinates: make([]float64, 0),
	}
}

func (g *Geo) FoundType(t string) bool {
	switch t {
	case "Point":
		fallthrough
	case "MultiPoint":
		fallthrough
	case "LineString":
		fallthrough
	case "MultiLineString":
		fallthrough
	case "Polygon":
		fallthrough
	case "MultiPolygon":
		fallthrough
	case "GeometryCollection":
		g.Type = t
		return true
	}
	return false
}

func (g *Geo) FoundCoordinate(c interface{}, err error) error {
	switch n := c.(type) {
	case float64:
		g.Coordinates = append(g.Coordinates, n)
	case int64:
		g.Coordinates = append(g.Coordinates, float64(n))
	}
	return err
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

func (q *Queue) Latest() *QueueQuad {
	return q.Waiting[len(q.Waiting)-1]
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

type Level struct {
	Type ParserState
	Uids []string
	Uid  string
}

func NewLevel(t ParserState, c uint64) *Level {
	return &Level{
		Type: t,
		Uids: make([]string, 0),
		Uid:  fmt.Sprintf("c.%d", c),
	}
}

func (l *Level) Subject() string {
	if len(l.Uids) == 0 {
		return l.Uid
	}
	return l.Uids[len(l.Uids)-1]
}

type Depth struct {
	Counter uint64
	Levels  []*Level
}

func NewDepth() *Depth {
	return &Depth{
		Levels: make([]*Level, 0),
	}
}

func (d *Depth) Down() *Level {
	return d.Levels[len(d.Levels)-2]
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
	cur := d.Levels[len(d.Levels)-1]
	cur.Uids = append(cur.Uids, uid)
}

func (d *Depth) Subject() string {
	return d.Levels[len(d.Levels)-1].Subject()
}

func (d *Depth) Increase(t ParserState) {
	if t == OBJECT {
		d.Counter++
	}
	d.Levels = append(d.Levels, NewLevel(t, d.Counter))
}

func (d *Depth) Decrease(t ParserState) *Level {
	top := d.Levels[len(d.Levels)-1]
	d.Levels = d.Levels[:len(d.Levels)-1]
	return top
}

func (d *Depth) String() string {
	o := ""
	for _, level := range d.Levels {
		if level.Type == OBJECT {
			o += "O "
		} else if level.Type == ARRAY {
			o += "A "
		} else {
			o += "? "
		}
	}
	return o
}

type Parser struct {
	State ParserState
	Quads []*Quad
	Queue *Queue
	Depth *Depth
	Geo   *Geo
	Quad  *Quad
	Skip  bool
}

func NewParser() *Parser {
	return &Parser{
		State: NONE,
		Quads: make([]*Quad, 0),
		Depth: NewDepth(),
		Queue: NewQueue(),
		Geo:   NewGeo(),
		Quad:  &Quad{},
	}
}

// Parse reads from the iterator until an error is raised or we reach the end of
// the tape.
func (p *Parser) Parse(iter json.Iter) ([]*Quad, error) {
	var err error
	for done := false; !done; {
		done, err = p.Scan(iter.AdvanceInto(), iter.PeekNextTag(), iter)
		if err != nil {
			return nil, err
		}
	}
	return p.Quads, nil
}

// Scan is called with the current (c) and next (n) simdjson.Tag on the tape.
// The Parser will continue reading from the tape and calling Scan until it
// returns true or an error.
//
// NOTE: only mutate p.State from within this function
func (p *Parser) Scan(c, n json.Tag, i json.Iter) (done bool, err error) {
	if p.Skip {
		p.Skip = false
		return
	}

	//defer p.Log(c, n)
	switch c {

	case json.TagString:
		switch p.State {
		case PREDICATE:
			if err = p.FoundPredicate(i.String()); err != nil {
				return
			}
			switch n {
			case json.TagObjectStart:
				p.State = OBJECT
				p.FoundSubject(OBJECT, p.Depth.Subject())
			case json.TagArrayStart:
				p.State = ARRAY
				p.FoundSubject(ARRAY, p.Depth.Subject())
			default:
				switch p.Quad.Predicate {
				case "uid":
					p.State = UID
				case "type":
					p.State = GEO
				default:
					p.State = SCALAR
				}
			}

		case SCALAR:
			p.State = PREDICATE
			if err = p.FoundValue(i.String()); err != nil {
				return
			}

		case UID:
			p.State = PREDICATE
			if err = p.FoundUid(i.String()); err != nil {
				return
			}

		case GEO:
			var s string
			if s, err = i.String(); err != nil {
				return
			}
			if p.Geo.FoundType(s) {
				p.State = GEO_COORDS
				p.Queue.Latest().Type = GEO
				p.Queue.Latest().Quad.Subject = p.Depth.Down().Uid
			} else {
				p.State = PREDICATE
			}

		case GEO_COORDS:
			var s string
			if s, err = i.String(); err != nil {
				return
			}
			if s != "coordinates" {
				// TODO: handle non-geo objects that just *look* like geo
				//       objects... as regular objects
			}
		}

	case json.TagFloat:
		switch p.State {
		case SCALAR:
			p.State = PREDICATE
			if err = p.FoundValue(i.Float()); err != nil {
				return
			}
		case GEO_COORDS:
			if err = p.Geo.FoundCoordinate(i.Float()); err != nil {
				return
			}
		}

	case json.TagUint, json.TagInteger:
		switch p.State {
		case SCALAR:
			p.State = PREDICATE
			if err = p.FoundValue(i.Int()); err != nil {
				return
			}
		case GEO_COORDS:
			if err = p.Geo.FoundCoordinate(i.Int()); err != nil {
				return
			}
		}

	case json.TagBoolFalse, json.TagBoolTrue:
		switch p.State {
		case SCALAR:
			p.State = PREDICATE
			if err = p.FoundValue(i.Bool()); err != nil {
				return
			}
		}

	case json.TagObjectStart:
		if n != json.TagObjectEnd {
			p.Depth.Increase(OBJECT)
		}
		switch n {
		case json.TagString:
			p.State = PREDICATE
		case json.TagObjectStart:
			p.State = OBJECT
		case json.TagObjectEnd:
			p.Queue.Pop(OBJECT)
			p.State = PREDICATE
			p.Skip = true
		case json.TagArrayStart:
			p.State = ARRAY
		}

	case json.TagArrayStart:
		if n != json.TagArrayEnd {
			p.Depth.Increase(ARRAY)
		}
		switch n {
		case json.TagObjectStart:
			p.State = OBJECT
		case json.TagArrayStart:
			p.State = ARRAY
		case json.TagArrayEnd:
			p.Queue.Pop(ARRAY)
			p.State = PREDICATE
			p.Skip = true
		}

	case json.TagObjectEnd:
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
				})
			}
		}
		switch n {
		case json.TagString:
			fallthrough
		case json.TagObjectEnd:
			fallthrough
		case json.TagArrayEnd:
			p.State = PREDICATE
		case json.TagObjectStart:
			p.State = OBJECT
		}

	case json.TagArrayEnd:
		if p.State == GEO_COORDS {
			if waiting := p.Queue.Pop(GEO); waiting != nil {
				p.Quads = append(p.Quads, &Quad{
					Subject:   waiting.Subject,
					Predicate: waiting.Predicate,
					ObjectVal: fmt.Sprintf("%v", p.Geo.Coordinates),
				})
			}
		}
		if !p.Queue.Empty() {
			if waiting := p.Queue.Pop(ARRAY); waiting != nil {
				uids := p.Depth.Decrease(ARRAY).Uids
				for _, uid := range uids {
					p.Quads = append(p.Quads, &Quad{
						Subject:   p.Depth.Subject(),
						Predicate: waiting.Predicate,
						ObjectId:  uid,
					})
				}
			}
		}
		switch n {
		case json.TagString:
			fallthrough
		case json.TagObjectEnd:
			fallthrough
		case json.TagArrayEnd:
			p.State = PREDICATE
		case json.TagArrayStart:
			p.State = ARRAY
		case json.TagObjectStart:
			p.State = OBJECT
		}

	case json.TagNull: // TODO
	case json.TagRoot:
		switch n {
		case json.TagObjectStart:
			p.State = OBJECT
		case json.TagArrayStart:
			p.State = ARRAY
		}

	case json.TagEnd:
		done = true
	}
	return
}

func (p *Parser) FoundUid(s string, err error) error {
	p.Depth.Uid(s)
	p.Quad = &Quad{}
	return err
}

func (p *Parser) FoundSubject(t ParserState, s string) {
	p.Queue.Add(t, p.Quad)
	p.Quad = &Quad{}
}

func (p *Parser) FoundPredicate(s string, err error) error {
	p.Quad.Predicate = s
	return err
}

func (p *Parser) FoundValue(v interface{}, err error) error {
	p.Quad.ObjectVal = v
	p.Quad.Subject = p.Depth.Subject()
	p.Quads = append(p.Quads, p.Quad)
	p.Quad = &Quad{}
	return err
}

func (p *Parser) Log(c, n json.Tag) {
	fmt.Println(c, n, p.Depth, p.State)
}

func Parse(d []byte) ([]*Quad, error) {
	tape, err := json.Parse(d, nil)
	if err != nil {
		return nil, err
	}
	return NewParser().Parse(tape.Iter())
}

type Quad struct {
	Subject   string
	Predicate string
	ObjectId  string
	ObjectVal interface{}
}
