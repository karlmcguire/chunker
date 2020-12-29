package chunker

import (
	"fmt"
	"os"
	"testing"
	"text/tabwriter"
)

type Case struct {
	Json string
	Quad []Quad
}

func Test1(t *testing.T) {
	c := &Case{
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
			Subject:   "1000",
			Predicate: "name",
			ObjectVal: "Bob",
		}, {
			Subject:   "c.1",
			Predicate: "friend",
			ObjectId:  "c.2",
		}, {
			Subject:   "c.1",
			Predicate: "friend",
			ObjectId:  "1000",
		}},
	}

	quads, err := Parse([]byte(c.Json))
	if err != nil {
		t.Fatal(err)
	}
	for i, quad := range quads {
		if quad.Subject != c.Quad[i].Subject {
			t.Fatal("bad subject")
		}
		if quad.Predicate != c.Quad[i].Predicate {
			t.Fatal("bad predicate")
		}
		if quad.ObjectId != c.Quad[i].ObjectId {
			t.Fatal("bad object id")
		}
		if quad.ObjectVal != c.Quad[i].ObjectVal {
			t.Fatal("bad object val")
		}
	}
}

func Test2(t *testing.T) {
	c := &Case{
		`{
			"name": "Alice",
			"address": {},
			"school": {
				"name": "Wellington Public School"
			}
		}`,

		[]Quad{{
			Subject:   "c.1",
			Predicate: "name",
			ObjectVal: "Alice",
		}, {
			Subject:   "c.2",
			Predicate: "name",
			ObjectVal: "Wellington Public School",
		}, {
			Subject:   "c.1",
			Predicate: "school",
			ObjectId:  "c.2",
		}},
	}

	quads, err := Parse([]byte(c.Json))
	if err != nil {
		t.Fatal(err)
	}
	for i, quad := range quads {
		if quad.Subject != c.Quad[i].Subject {
			t.Fatal("bad subject")
		}
		if quad.Predicate != c.Quad[i].Predicate {
			t.Fatal("bad predicate")
		}
		if quad.ObjectId != c.Quad[i].ObjectId {
			t.Fatal("bad object id")
		}
		if quad.ObjectVal != c.Quad[i].ObjectVal {
			t.Fatal("bad object val")
		}
	}
}

func Test3(t *testing.T) {
	c := &Case{
		`[
			{
				"name": "Alice",
				"mobile": "040123456",
				"car": "MA0123", 
				"age": 21, 
				"weight": 58.7
			}
		]`,
		[]Quad{
			{"c.1", "name", "", "Alice"},
			{"c.1", "mobile", "", "040123456"},
			{"c.1", "car", "", "MA0123"},
			{"c.1", "age", "", 21},
			{"c.1", "weight", "", 58.7},
		},
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "subj\tpred\to_id\to_val\n")
	fmt.Fprintf(w, "----\t----\t-----\t----\n")
	quads, err := Parse([]byte(c.Json))
	if err != nil {
		t.Fatal(err)
	}
	for i, quad := range quads {
		fmt.Fprintf(w, "%s\t%s\t%s\t%v\n",
			quad.Subject, quad.Predicate, quad.ObjectId, quad.ObjectVal)
		if quad.Subject != c.Quad[i].Subject {
			t.Fatal("bad subject")
		}
		if quad.Predicate != c.Quad[i].Predicate {
			t.Fatal("bad predicate")
		}
		if quad.ObjectId != c.Quad[i].ObjectId {
			t.Fatal("bad object id")
		}
		if fmt.Sprintf("%v", quad.ObjectVal) != fmt.Sprintf("%v", c.Quad[i].ObjectVal) {
			t.Fatal("bad object val")
		}
	}
	w.Flush()
}
