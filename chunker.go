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

func Parse(d []byte) ([]*Quad, error) {
	tape, err := simdjson.Parse(d, nil)
	if err != nil {
		return nil, err
	}

	quad := &Quad{}
	buff := make([]*Quad, 0)
	iter := tape.Iter()

	openValue := false
	openObject := false
	openArray := false

	for {
		tag := iter.AdvanceInto()

		switch tag {

		case simdjson.TagString:
			if openValue {
				openValue = false
				quad.ObjectVal, _ = iter.String()
				buff = append(buff, quad)
				quad = &Quad{}
			} else {
				quad.Predicate, _ = iter.String()
				next := iter.PeekNextTag()
				if next == simdjson.TagObjectStart {
					openObject = true
				} else if next == simdjson.TagArrayStart {
					openArray = true
				} else {
					openValue = true
				}
			}

		case simdjson.TagInteger:
			if openValue {
				openValue = false
				quad.ObjectVal, _ = iter.Int()
				buff = append(buff, quad)
				quad = &Quad{}
			}

		case simdjson.TagUint:
			if openValue {
				openValue = false
				quad.ObjectVal, _ = iter.Uint()
				buff = append(buff, quad)
				quad = &Quad{}
			}

		case simdjson.TagFloat:
			if openValue {
				openValue = false
				quad.ObjectVal, _ = iter.Float()
				buff = append(buff, quad)
				quad = &Quad{}
			}

		case simdjson.TagBoolTrue:
			fallthrough
		case simdjson.TagBoolFalse:
			if openValue {
				openValue = false
				quad.ObjectVal, _ = iter.Bool()
				buff = append(buff, quad)
				quad = &Quad{}
			}

		case simdjson.TagObjectStart:
		case simdjson.TagObjectEnd:
			openObject = false

		case simdjson.TagArrayStart:
		case simdjson.TagArrayEnd:
			openArray = false

		case simdjson.TagNull:
		case simdjson.TagRoot:
		case simdjson.TagEnd:
			return buff, nil
		}

		fmt.Println(tag, openValue, openObject, openArray)
	}
	return buff, nil
}
