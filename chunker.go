package chunker

import (
	"fmt"

	json "github.com/minio/simdjson-go"
)

/*
r { OBJECT
{ " PREDICATE
" " SCALAR
" " PREDICATE
" { OBJECT
{ } PREDICATE
} " PREDICATE
" [ ARRAY
[ { OBJECT
{ " PREDICATE
" " SCALAR
" " PREDICATE
" f SCALAR
f " PREDICATE
" { OBJECT
{ } PREDICATE
} } PREDICATE
} { OBJECT
{ " PREDICATE
" " SCALAR
" " PREDICATE
" " SCALAR
" " PREDICATE
" { OBJECT
{ } PREDICATE
} } PREDICATE
} ] PREDICATE
] } PREDICATE
} r DONE
r   DONE
    DONE
*/

type Status uint8

const (
	PREDICATE Status = iota
	SCALAR
	OBJECT
	ARRAY
)

type Walk struct {
	Status Status
	Quad   *Quad
	Quads  []*Quad
}

func NewWalk() *Walk {
	return &Walk{
		Quad:  &Quad{},
		Quads: make([]*Quad, 0),
	}
}

func (w *Walk) Push() {
	w.Quads = append(w.Quads, w.Quad)
	w.Quad = &Quad{}
}

func (w *Walk) Read(i json.Iter, t, n json.Tag) bool {
	defer fmt.Println(t, n, w.Status)
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
				w.Status = SCALAR
			}

		case SCALAR:
			w.Quad.ObjectVal, _ = i.String()
			w.Push()
			w.Status = PREDICATE

		case OBJECT:
			fmt.Println("shouldn't happen (object)")
		case ARRAY:
			fmt.Println("shouldn't happen (array)")
		}

	case json.TagInteger:
	case json.TagUint:
	case json.TagFloat:
	case json.TagBoolTrue:
	case json.TagBoolFalse:
	case json.TagObjectStart:
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
			// TODO: should never happen
		case json.TagArrayEnd:
			// TODO: should never happen
		case json.TagNull:
			// TODO: should never happen
		case json.TagRoot:
			// TODO: should never happen
		case json.TagEnd:
			// TODO: should never happen
		}
	case json.TagObjectEnd:
	case json.TagArrayStart:
	case json.TagArrayEnd:
	case json.TagNull:
	case json.TagRoot:
	case json.TagEnd:
		return true
	}
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
