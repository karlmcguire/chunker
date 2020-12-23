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


r
|
openValue  = false
openObject = false -> true
openArray  = false
openPred   = false


{
|
openValue  = false
openObject = true -> false (previously was true set by root)
openArray  = false
openPred   = false -> true (next tag is string)


    "name": "Alice",
	|       |
	openValue  = false -> true (next tag is scalar)
	openObject = false
	openArray  = false
	openPred   = true -> false (openValue is true)
	        |
			openValue  = true -> false (found the value)
			openObject = false
			openArray  = false
			openPred   = false -> true (found the value)


	"address": {},
	|          |
	openValue  = false
	openObject = false -> true (next tag is ObjectStart)
	openArray  = false
	openPred   = true -> false (openObject is true)
	           |
			   openValue  = false
			   openObject = true -> false (next tag is ObjectClose)
			   openValue  = false
			   openPred   = false -> true (found the value)


	"friend": [
	|         |
	openValue  = false
	openObject = false
	openArray  = false -> true (next value is ArrayStart)
	openPred   = true -> false (openArray is true)
	          |
			  openValue  = false
			  openObject = false
			  openArray  = true -> false
			  openPred   = false -> true


		{
		|
		openValue  = false
		openObject = false
		openArray  = false
		openPred   = true (next value is string)


			"name": "Charlie",
			|       |
			openValue  = false
			openObject = false
			openArray  = false
			openPred   = false
					|
					openValue  = false
					openObject = false
					openArray  = false
					openPred   = false


			"married": false,
			|          |
			openValue  = false
			openObject = false
			openArray  = false
			openPred   = false
					   |
					   openValue  = false
					   openObject = false
					   openArray  = false
					   openPred   = false


			"address": {}
			|          |
			openValue  = false
			openObject = false
			openArray  = false
			openPred   = false
					   |
					   openValue  = false
					   openObject = false
					   openArray  = false
					   openPred   = false


		}, {
		|  |
		openValue  = false
		openObject = false
		openArray  = false
		openPred   = false
		   |
		   openValue  = false
		   openObject = false
		   openArray  = false
		   openPred   = false


			"uid": "1000",
		    |      |
		    openValue  = false
		    openObject = false
		    openArray  = false
		    openPred   = false
				   |
				   openValue  = false
				   openObject = false
				   openArray  = false
				   openPred   = false


			"name": "Bob",
			|       |
			openValue  = false
			openObject = false
			openArray  = false
			openPred   = false
			        |
					openValue  = false
					openObject = false
					openArray  = false
					openPred   = false


			"address": {}
			|          |
			openValue  = false
			openObject = false
			openArray  = false
			openPred   = false
			           |
					   openValue  = false
					   openObject = false
					   openArray  = false
					   openPred   = false


		}
		|
		openValue  = false
		openObject = false
		openArray  = false
		openPred   = false


	]
	|
	openValue  = false
	openObject = false
	openArray  = false
	openPred   = false


}
|
openValue  = false
openObject = false
openArray  = false
openPred   = false

*/

type Quad struct {
	Subject   string
	Predicate string
	ObjectId  string
	ObjectVal interface{}
}

type walk struct {
	curr       *Quad
	quads      []*Quad
	waits      []*Quad
	openValue  bool
	openObject bool
	openArray  bool
	openPred   bool
	skip       bool
}

func newWalk() *walk {
	return &walk{
		curr:  &Quad{},
		quads: make([]*Quad, 0),
		waits: make([]*Quad, 0),
	}
}

func Parse(d []byte) ([]*Quad, error) {
	tape, err := simdjson.Parse(d, nil)
	if err != nil {
		return nil, err
	}

	iter := tape.Iter()
	walk := newWalk()

	for {
		t, n := iter.AdvanceInto(), iter.PeekNextTag()

		if walk.skip {
			walk.skip = false
			continue
		}

		switch n {
		case simdjson.TagString:
			fallthrough
		case simdjson.TagInteger:
			fallthrough
		case simdjson.TagUint:
			fallthrough
		case simdjson.TagFloat:
			fallthrough
		case simdjson.TagBoolTrue:
			fallthrough
		case simdjson.TagBoolFalse:
			if walk.openPred {
				walk.openPred = false
				walk.openValue = true
			} else {
				walk.openPred = true
				walk.openValue = false
			}

		case simdjson.TagObjectStart:
			walk.openObject = true
			if walk.openPred {
				walk.openPred = false
			}

		case simdjson.TagArrayStart:
			walk.openArray = true
			if walk.openPred {
				walk.openPred = false
			}
		}

		switch t {
		case simdjson.TagString:
			if walk.openPred {
				walk.openPred = false
				walk.curr.Predicate, _ = iter.String()
			} else if walk.openValue {
				walk.openValue = false
				walk.curr.ObjectVal, _ = iter.String()
				walk.quads = append(walk.quads, walk.curr)
				walk.curr = &Quad{}
			}
		case simdjson.TagInteger:
		case simdjson.TagUint:
		case simdjson.TagFloat:
		case simdjson.TagBoolTrue:
		case simdjson.TagBoolFalse:

		case simdjson.TagObjectStart:
			walk.openObject = false
			if n == simdjson.TagObjectEnd {
				walk.skip = true
			} else {

			}

		case simdjson.TagObjectEnd:

		case simdjson.TagArrayStart:
			walk.openArray = false
			if n == simdjson.TagArrayEnd {
				walk.skip = true
			} else {
				if walk.openValue {
					walk.waits = append(walk.waits, walk.curr)
					walk.curr = &Quad{}
				}
			}

		case simdjson.TagArrayEnd:

		case simdjson.TagNull:
		case simdjson.TagRoot:
		case simdjson.TagEnd:
			return walk.quads, nil
		}

		fmt.Println(t, n)
		fmt.Printf("openValue  = %v\nopenObject = %v\nopenArray  = %v\nopenPred   = %v\nskip       = %v\n\n",
			walk.openValue, walk.openObject, walk.openArray, walk.openPred, walk.skip)

	}

	spew.Dump(walk.waits)

	return walk.quads, nil
}
