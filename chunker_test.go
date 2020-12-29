package chunker

import (
	"fmt"
	"os"
	"testing"
	"text/tabwriter"
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

type Case struct {
	Json string
	Quad []Quad
}

func Test(t *testing.T) {
	cases := []Case{
		{
			`{
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
			}`,
			[]Quad{{
				Subject:   "c.1",
				Predicate: "name",
				ObjectVal: "Alice",
			}, {
				Subject:   "c.2",
				Predicate: "name",
				ObjectVal: "Charlie",
			}, {
				Subject:   "c.2",
				Predicate: "married",
				ObjectVal: false,
			}, {
				Subject:   "c.1",
				Predicate: "friend",
				ObjectId:  "c.2",
			}, {
				Subject:   "1000",
				Predicate: "name",
				ObjectVal: "Bob",
			}, {
				Subject:   "c.1",
				Predicate: "friend",
				ObjectId:  "1000",
			}},
		},
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "subj\tpred\to_id\to_val\n")
	fmt.Fprintf(w, "----\t----\t-----\t----\n")
	for _, c := range cases {
		quads, err := Parse([]byte(c.Json))
		if err != nil {
			t.Fatal(err)
		}
		for _, quad := range quads {
			fmt.Fprintf(w, "%s\t%s\t%s\t%v\n",
				quad.Subject, quad.Predicate, quad.ObjectId, quad.ObjectVal)
		}
	}
	fmt.Println()
	w.Flush()
	fmt.Println()
}
