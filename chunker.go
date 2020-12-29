package chunker

import (
	"fmt"

	json "github.com/minio/simdjson-go"
)

type Quad struct {
	Subject   string
	Predicate string
	ObjectId  string
	ObjectVal interface{}
}

var subjectCounter uint64

func getNextBlank() string {
	subjectCounter++
	return fmt.Sprintf("c.%d", subjectCounter)
}

type Status uint8

const (
	PREDICATE Status = iota
	SCALAR
	OBJECT
	ARRAY
	UID
)

type Level struct {
	Type Status
	Uid  []string
}

// TODO; this seems like a weird function to need/have
func (l *Level) Top() string {
	if len(l.Uid) == 0 {
		return ""
	}
	return l.Uid[len(l.Uid)-1]
}

type Walk struct {
	Status     Status
	Quad       *Quad
	Quads      []*Quad
	Level      []*Level
	Skip       bool
	WaitArray  []*Quad
	WaitObject []*Quad
}

func NewWalk() *Walk {
	return &Walk{
		Status:     OBJECT,
		Quad:       &Quad{},
		Quads:      make([]*Quad, 0),
		Level:      make([]*Level, 0),
		WaitArray:  make([]*Quad, 0),
		WaitObject: make([]*Quad, 0),
	}
}

func (w *Walk) Push() {
	w.Quad.Subject = w.Level[len(w.Level)-1].Top()
	w.Quads = append(w.Quads, w.Quad)
	w.Quad = &Quad{}
}

// TODO: manage WaitObject stack similar to the way WaitArray is already managed
//       and if anything it should be easier because we don't have to deal with
//       multiple reference quads like with arrays (if we need to do this,
//       haven't checked the spec yet)
//
// TODO: performance tuning
func (w *Walk) Read(i json.Iter, t, n json.Tag) bool {
	if w.Skip {
		w.Skip = false
		return false
	}

	switch t {
	case json.TagString:
		switch w.Status {
		case PREDICATE:
			w.Quad.Predicate, _ = i.String()
			switch n {
			case json.TagObjectStart:
				if len(w.Level) > 0 {
					w.Quad.Subject = w.Level[len(w.Level)-1].Top()
				}
				w.WaitObject = append(w.WaitObject, w.Quad)
				w.Quad = &Quad{}
				w.Status = OBJECT
			case json.TagArrayStart:
				if len(w.Level) > 0 {
					w.Quad.Subject = w.Level[len(w.Level)-1].Top()
				}
				w.WaitArray = append(w.WaitArray, w.Quad)
				w.Quad = &Quad{}
				w.Status = ARRAY
			default:
				if w.Quad.Predicate == "uid" {
					w.Status = UID
				} else {
					w.Status = SCALAR
				}
			}
		case SCALAR:
			w.Quad.ObjectVal, _ = i.String()
			w.Push()
			w.Status = PREDICATE
		case UID:
			// TODO: if we find a uid, we need to delete the previously
			//       generated uid (c.* string from getNextBlank function) so
			//       that when we check w.Wait for nquads referencing this
			//       object we can use the correct uid
			s, _ := i.String()
			curr := w.Level[len(w.Level)-1]
			curr.Uid[len(curr.Uid)-1] = s
			w.Quad = &Quad{}
			w.Status = PREDICATE
		}

	case json.TagInteger:
		if w.Status == SCALAR {
			w.Quad.ObjectVal, _ = i.Int()
			w.Push()
			w.Status = PREDICATE
		}

	case json.TagUint:
		if w.Status == SCALAR {
			w.Quad.ObjectVal, _ = i.Int()
			w.Push()
			w.Status = PREDICATE
		}

	case json.TagFloat:
		if w.Status == SCALAR {
			w.Quad.ObjectVal, _ = i.Float()
			w.Push()
			w.Status = PREDICATE
		}

	case json.TagBoolTrue:
		fallthrough
	case json.TagBoolFalse:
		if w.Status == SCALAR {
			w.Quad.ObjectVal, _ = i.Bool()
			w.Push()
			w.Status = PREDICATE
		}

	case json.TagObjectStart:
		if n != json.TagObjectEnd {
			w.Level = append(w.Level, &Level{
				Type: OBJECT,
				Uid:  []string{getNextBlank()},
			})
		}
		switch n {
		case json.TagString:
			fallthrough
		case json.TagInteger:
			fallthrough
		case json.TagUint:
			fallthrough
		case json.TagFloat:
			fallthrough
		case json.TagBoolTrue:
			fallthrough
		case json.TagBoolFalse:
			w.Status = PREDICATE
		case json.TagObjectStart:
			w.Status = OBJECT
		case json.TagArrayStart:
			w.Status = ARRAY
		case json.TagObjectEnd:
			w.Status = PREDICATE
			w.Skip = true
			w.WaitObject = w.WaitObject[:len(w.WaitObject)-1]
		}

	case json.TagArrayStart:
		if n != json.TagArrayEnd {
			w.Level = append(w.Level, &Level{
				Type: ARRAY,
				Uid:  make([]string, 0),
			})
		}
		switch n {
		case json.TagString:
			fallthrough
		case json.TagInteger:
			fallthrough
		case json.TagUint:
			fallthrough
		case json.TagFloat:
			fallthrough
		case json.TagBoolTrue:
			fallthrough
		case json.TagBoolFalse:
			w.Status = PREDICATE
		case json.TagObjectStart:
			w.Status = OBJECT
		case json.TagArrayStart:
			w.Status = ARRAY
		case json.TagArrayEnd:
			w.Status = PREDICATE
			w.Skip = true
			w.WaitArray = w.WaitArray[:len(w.WaitArray)-1]
		}

	case json.TagObjectEnd:
		if len(w.WaitArray) > 0 {
			if len(w.Level) >= 2 {
				curr := w.Level[len(w.Level)-1]
				under := w.Level[len(w.Level)-2]
				if under.Type == ARRAY {
					under.Uid = append(under.Uid, curr.Uid[0])
				}
			}
		}
		w.Level = w.Level[:len(w.Level)-1]
		switch n {
		case json.TagObjectStart:
			w.Status = OBJECT
		default:
			w.Status = PREDICATE
		}

	case json.TagArrayEnd:
		if len(w.WaitArray) > 0 {
			wait := w.WaitArray[len(w.WaitArray)-1]
			uids := w.Level[len(w.Level)-1].Uid
			for _, uid := range uids {
				quad := &Quad{
					Subject:   wait.Subject,
					Predicate: wait.Predicate,
					ObjectId:  uid,
				}
				w.Quads = append(w.Quads, quad)
			}
			w.WaitArray = w.WaitArray[:len(w.WaitArray)-1]
		}
		w.Level = w.Level[:len(w.Level)-1]
		switch n {
		case json.TagArrayStart:
			w.Status = ARRAY
		default:
			w.Status = PREDICATE
		}

	case json.TagNull:
	case json.TagRoot:
		switch n {
		case json.TagObjectStart:
			w.Status = OBJECT
		case json.TagArrayStart:
			w.Status = ARRAY
		}

	case json.TagEnd:
		return true
	}

	return false
}

func Parse(d []byte) ([]*Quad, error) {
	tape, err := json.Parse(d, nil)
	if err != nil {
		return nil, err
	}

	walk := NewWalk()

	done := false
	for iter := tape.Iter(); !done; {
		done = walk.Read(iter, iter.AdvanceInto(), iter.PeekNextTag())
	}

	return walk.Quads, nil
}
