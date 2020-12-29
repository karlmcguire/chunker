package chunker

import (
	"testing"
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
