package chunker

import (
	"fmt"

	json "github.com/minio/simdjson-go"
)

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
	Uid  string
}

type Walk struct {
	Status Status
	Quad   *Quad
	Quads  []*Quad
	Level  []*Level
	Skip   bool
}

func NewWalk() *Walk {
	return &Walk{
		Status: OBJECT,
		Quad:   &Quad{},
		Quads:  make([]*Quad, 0),
		Level:  make([]*Level, 0),
	}
}

func (w *Walk) Push() {
	w.Quad.Subject = w.Level[len(w.Level)-1].Uid
	w.Quads = append(w.Quads, w.Quad)
	w.Quad = &Quad{}
}

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
				w.Status = OBJECT
			case json.TagArrayStart:
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
			w.Level[len(w.Level)-1].Uid, _ = i.String()
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
		// TODO: subjects
		//
		// some way to set subject per depth
		if n != json.TagObjectEnd {
			w.Level = append(w.Level, &Level{
				Type: OBJECT,
				Uid:  getNextBlank(),
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
		}

	case json.TagArrayStart:
		if n != json.TagArrayEnd {
			w.Level = append(w.Level, &Level{
				Type: ARRAY,
				// TODO: not sure if i should do getNextBlank() for arrays?
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
		}

	case json.TagObjectEnd:
		w.Level = w.Level[:len(w.Level)-1]
		switch n {
		case json.TagObjectStart:
			w.Status = OBJECT
		default:
			w.Status = PREDICATE
		}

	case json.TagArrayEnd:
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
		fmt.Println(t, n, w.Level, w.Status)
		return true
	}

	tl := &Level{}
	l := len(w.Level)
	if l != 0 {
		tl = w.Level[l-1]
	}
	fmt.Println(t, n, l, tl, w.Status)
	return false
}

type Quad struct {
	Subject   string
	Predicate string
	ObjectId  string
	ObjectVal interface{}
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
