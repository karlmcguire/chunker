package chunker

import (
	"fmt"

	"github.com/davecgh/go-spew/spew"
	"github.com/minio/simdjson-go"
)

/*
{
	"name": "Alice",
	"address": {},
	"friend": [
		{
			"name": "Charlie",
			"married": false,
			"address": {}
		}, {
			"uid": "1000",
			"name": "Bob",
			"address": {}
		}
	]
}


{
|
V
open for object
	- look for "uid" field


	"name": "Alice",
	  |       |
	  V       |
     if next == scalar {
		open for value
	 }        |
	          V
			 close for value (depending on context, push to buffer)


	"address": {},
      |        |
	  V        |
	 if next != scalar {
		open for object
	 }         |
               V
			  if next == object close {
				close for object (ignore close bracket)
			  }


	"friend": [
      |       |
	  V       |
 	 if next != scalar {
 		open for object
 	 }        |
              V
 			 if next != array close {
 				increase level
 			 }


		{
        |
		V
	   if next != object close {
			increase level
	   }


			"name": "Charlie",
              |       |
			  V       |
			 if next == scalar {
				open for value
			 }        |
                      V
					 close for value


			"married": false,
			  |         |
			  V         |
			 if next == scalar {
				open for value
			 }          |
                        V
					   close for value


			"address": {}
			  |        |
			  V        |
			 if next != scalar {
				open for object
			 }         |
                       V
					  if next == object close {
						close for object (ignore closing bracket)
					  }


		}, {
        |  |
		V  |
	   close for object
           |
		   V
		  increase level


			"uid": "1000",
              |      |
			  V      |
			 if next == scalar {
				open for value
				if "uid" && no uid for this object yet {
					open for uid
				}    |
			 }       |
			         V
					if open for uid {
						close for value
						close for uid
					}


			"name": "Bob",
              |       |
			  V       |
			 if next == scalar {
				open for value
			 }        |
			          V
					 close for value

			"address": {}
			  |        |
			  V        |
			 if next == object open {
				open for object
			 }         |
			           V
					   if next == object close {
							close for object (ignore closing bracket)
					   }


		}
		|
		V
	   close for object


	]
	|
	V
   close for array


}
|
V
close for object


*/

type Quad struct {
	Subject   string
	Predicate string
	ObjectId  string
	ObjectVal interface{}
}

/*
(subject:"_:dg.3267536919.1" predicate:"name" object_value:<str_val:"Alice" > )
(subject:"_:dg.3267536919.2" predicate:"name" object_value:<str_val:"Charlie" > ),
(subject:"_:dg.3267536919.2" predicate:"married" object_value:<bool_val:false > ),
(subject:"_:dg.3267536919.1" predicate:"friend" object_id:"1000" ),
(subject:"1000" predicate:"name" object_value:<str_val:"Bob" > ),
(subject:"_:dg.3267536919.1" predicate:"friend" object_id:"_:dg.3267536919.2" ),
*/

var counter = 0

func getNextBlank() string {
	counter++
	return fmt.Sprintf("c.%d", counter)
}

type levelOf uint8

const (
	OBJECT levelOf = iota
	ARRAY
)

type level struct {
	of    levelOf
	id    string
	uid   string
	quads uint64
}

type walk struct {
	curr    *Quad
	quads   []*Quad
	waiting []*Quad
	levels  []*level

	openValue  bool
	openObject bool
	openArray  bool
	openUid    bool
	skip       bool
}

func newWalk() *walk {
	return &walk{
		curr:    &Quad{},
		quads:   make([]*Quad, 0),
		waiting: make([]*Quad, 0),
		levels:  make([]*level, 0),
	}
}

func (w *walk) getLevel() *level {
	if len(w.levels) >= 1 {
		return w.levels[len(w.levels)-1]
	}
	return nil
}

func (w *walk) getLevelUp() *level {
	if len(w.levels) >= 2 {
		return w.levels[len(w.levels)-2]
	}
	return nil
}

func (w *walk) lookingForPred() bool {
	return w.openObject && !w.openValue && !w.openUid
}

func (w *walk) foundPred(p string, t simdjson.Tag) {
	if w.curr.Predicate != "" {
		fmt.Println("replacing ", w.curr.Predicate, " with ", p)
	}
	w.curr.Predicate = p
	if t != simdjson.TagObjectStart && t != simdjson.TagArrayStart {
		w.openValue = true
	} else {
		w.waiting = append(w.waiting, w.curr)
	}
}

func (w *walk) lookingForVal() bool {
	return w.openValue
}

func (w *walk) foundVal() {
	w.openValue = false

	// increase the count of the current level
	level := w.getLevel()
	if level != nil {
		level.quads++
	}

	// if we're within an array, we also want to increase the total count there
	levelUp := w.getLevelUp()
	if levelUp != nil && levelUp.of == ARRAY {
		levelUp.quads++
	}

	if level.uid != "" {
		w.curr.Subject = level.uid
	}
	w.quads = append(w.quads, w.curr)
	w.curr = &Quad{}
}

func (w *walk) foundValString(v string) {
	w.curr.ObjectVal = v
	w.foundVal()
}

func (w *walk) foundValInt(v int64) {
	w.curr.ObjectVal = v
	w.foundVal()
}

func (w *walk) foundValFloat(v float64) {
	w.curr.ObjectVal = v
	w.foundVal()
}

func (w *walk) foundValBool(v bool) {
	w.curr.ObjectVal = v
	w.foundVal()
}

func (w *walk) lookingForUid() bool {
	return w.openObject && !w.openUid
}

func (w *walk) foundUid() {
	w.openUid = true
}

func (w *walk) lookingForUidVal() bool {
	return w.openObject && w.openUid
}

func (w *walk) foundUidVal(v string) {
	w.getLevel().uid = v
	w.openUid = false
}

func (w *walk) foundEmptyObject() {
	w.skip = true
	w.openValue = false

	if len(w.waiting) > 0 {
		if w.waiting[len(w.waiting)-1] == w.curr {
			fmt.Println("deleting ", w.curr.Predicate)
			w.waiting = w.waiting[:len(w.waiting)-1]
		}
	}

	w.curr = &Quad{}
}

func (w *walk) foundEmptyArray() {
	w.skip = true
	w.openValue = false

	if len(w.waiting) > 0 {
		if w.waiting[len(w.waiting)-1] == w.curr {
			w.waiting = w.waiting[:len(w.waiting)-1]
		}
	}

	w.curr = &Quad{}
}

func Parse(d []byte) ([]*Quad, error) {
	tape, err := simdjson.Parse(d, nil)
	if err != nil {
		return nil, err
	}

	iter := tape.Iter()
	walk := newWalk()

	for {
		tag := iter.AdvanceInto()

		if walk.skip {
			walk.skip = false
			continue
		}

		switch tag {
		case simdjson.TagString:
			k, err := iter.String()
			if err != nil {
				panic(err)
			}
			if walk.lookingForUidVal() {
				walk.foundUidVal(k)
			} else if walk.lookingForUid() && k == "uid" {
				walk.foundUid()
			} else if walk.lookingForVal() {
				walk.foundValString(k)
			} else if walk.lookingForPred() {
				walk.foundPred(k, iter.PeekNextTag())
			}

		case simdjson.TagInteger:
			v, err := iter.Int()
			if err != nil {
				panic(err)
			}
			if walk.lookingForVal() {
				walk.foundValInt(v)
			}

		case simdjson.TagUint:
			v, err := iter.Uint()
			if err != nil {
				panic(err)
			}
			if walk.lookingForVal() {
				walk.foundValInt(int64(v))
			}

		case simdjson.TagFloat:
			v, err := iter.Float()
			if err != nil {
				panic(err)
			}
			if walk.lookingForVal() {
				walk.foundValFloat(v)
			}

		case simdjson.TagBoolTrue:
			fallthrough
		case simdjson.TagBoolFalse:
			v, err := iter.Bool()
			if err != nil {
				panic(err)
			}
			if walk.lookingForVal() {
				walk.foundValBool(v)
			}

		case simdjson.TagObjectStart:
			next := iter.PeekNextTag()
			if next == simdjson.TagObjectEnd {
				walk.foundEmptyObject()
			} else {
				walk.openObject = true
				walk.levels = append(walk.levels, &level{
					of: OBJECT,
					id: getNextBlank(),
				})
				if walk.lookingForVal() {
					walk.waiting = append(walk.waiting, walk.curr)
				}
			}

		case simdjson.TagObjectEnd:
			if walk.openObject {
				walk.openObject = false
				level := walk.getLevel()
				if level.uid == "" {
					for i := uint64(0); i < level.quads; i++ {
						walk.quads[uint64(len(walk.quads))-1-i].Subject = level.id
					}
				} else {
					// TODO: replace ObjectId for predicates referencing the
					//       current object
					/*
						if len(walk.waiting) > 0 {
							walk.waiting[len(walk.waiting)-1].ObjectId = level.uid
							walk.waiting = walk.waiting[:len(walk.waiting)-1]
						}
					*/
				}
				walk.levels = walk.levels[:len(walk.levels)-1]
			}

		case simdjson.TagArrayStart:
			next := iter.PeekNextTag()
			if next == simdjson.TagArrayEnd {
				walk.foundEmptyArray()
			} else {
				walk.openArray = true
				walk.levels = append(walk.levels, &level{
					of: ARRAY,
					// don't generate a new id, those are only for objects
				})
			}

		case simdjson.TagArrayEnd:
			if walk.openArray {
				walk.openArray = false
			}

		case simdjson.TagNull:
		case simdjson.TagRoot:
		case simdjson.TagEnd:
			return walk.quads, nil
		}

		fmt.Println(tag, walk.openValue, walk.openObject, walk.openArray)
		spew.Dump(walk.waiting)
		fmt.Println()
		fmt.Println()
		fmt.Println()
	}
	return walk.quads, nil
}
