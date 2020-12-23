package chunker

import (
	"fmt"

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

type object struct {
	uid string
}

type walk struct {
	objects []*object

	curr       *Quad
	quads      []*Quad
	openValue  bool
	openObject bool
	openArray  bool
	openUid    bool
}

func newWalk() *walk {
	return &walk{
		objects: make([]*object, 0),
		quads:   make([]*Quad, 0),
		curr:    &Quad{},
	}
}

func (w *walk) lookingForPred() bool {
	return w.openObject && !w.openValue && !w.openArray && !w.openUid
}

func (w *walk) foundPred(p string) {
	w.openValue = true
	w.curr.Predicate = p
}

func (w *walk) lookingForVal() bool {
	return w.openValue
}

func (w *walk) foundVal() {
	w.openValue = false
	if len(w.objects) > 0 {
		if uid := w.objects[len(w.objects)-1].uid; uid != "" {
			w.curr.Subject = uid
		}
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
	w.objects[len(w.objects)-1].uid = v
	w.openUid = false
}

func (w *walk) foundEmptyObject() {
	w.curr = &Quad{}
}

func (w *walk) String() string {
	o := ""
	for i := range w.objects {
		o += fmt.Sprintf("'%s', ", w.objects[i].uid)
	}
	if len(o) == 0 {
		return ""
	}
	return o[:len(o)-2]
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
				walk.foundPred(k)
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
				walk.objects = append(walk.objects, &object{})
			}

		case simdjson.TagObjectEnd:
			if walk.openObject {
				walk.openObject = false
				walk.objects = walk.objects[:len(walk.objects)-1]
			}

		case simdjson.TagArrayStart:
		case simdjson.TagArrayEnd:

		case simdjson.TagNull:
		case simdjson.TagRoot:
		case simdjson.TagEnd:
			return walk.quads, nil
		}

		fmt.Println(tag, walk)
	}
	return walk.quads, nil
}
