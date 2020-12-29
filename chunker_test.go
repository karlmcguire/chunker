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

func TestGeo(t *testing.T) {
	c := &Case{
		`{
			"name": "Alice",
			"age": 26,
			"married": true,
			"now": "2020-12-29T17:39:34.816808024Z",
			"address": {
				"type": "Point",
				"coordinates": [
					1.1, 
					2
				]
			}
		}`,

		[]Quad{
			{"c.1", "name", "", "Alice"},
			{"c.1", "age", "", 26},
			{"c.1", "married", "", true},
			{"c.1", "now", "", "2020-12-29T17:39:34.816808024Z"},
			{"c.1", "address", "", "geoval"}, // TODO: geoval parsing
		},
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "subj\tpred\to_id\to_val\n")
	fmt.Fprintf(w, "----\t----\t-----\t----\n")
	quads, err := Parse([]byte(c.Json))
	if err != nil {
		t.Fatal(err)
	}
	for _, quad := range quads {
		fmt.Fprintf(w, "%s\t%s\t%s\t%v\n",
			quad.Subject, quad.Predicate, quad.ObjectId, quad.ObjectVal)
	}
	w.Flush()
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

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "subj\tpred\to_id\to_val\n")
	fmt.Fprintf(w, "----\t----\t-----\t----\n")
	quads, err := Parse([]byte(c.Json))
	if err != nil {
		t.Fatal(err)
	}
	for _, quad := range quads {
		fmt.Fprintf(w, "%s\t%s\t%s\t%v\n",
			quad.Subject, quad.Predicate, quad.ObjectId, quad.ObjectVal)
	}
	w.Flush()
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

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "subj\tpred\to_id\to_val\n")
	fmt.Fprintf(w, "----\t----\t-----\t----\n")
	quads, err := Parse([]byte(c.Json))
	if err != nil {
		t.Fatal(err)
	}
	for _, quad := range quads {
		fmt.Fprintf(w, "%s\t%s\t%s\t%v\n",
			quad.Subject, quad.Predicate, quad.ObjectId, quad.ObjectVal)
	}
	w.Flush()
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
	for _, quad := range quads {
		fmt.Fprintf(w, "%s\t%s\t%s\t%v\n",
			quad.Subject, quad.Predicate, quad.ObjectId, quad.ObjectVal)
	}
	w.Flush()
}

func Test4(t *testing.T) {
	c := &Case{
		`{
			"name": "Alice",
			"age": 25,
			"friends": [
				{
					"name": "Bob"
				}
			]
		}`,
		[]Quad{
			{"c.1", "name", "", "Alice"},
			{"c.1", "age", "", 25},
			{"c.2", "name", "", "Bob"},
			{"c.1", "friends", "c.2", nil},
		},
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "subj\tpred\to_id\to_val\n")
	fmt.Fprintf(w, "----\t----\t-----\t----\n")
	quads, err := Parse([]byte(c.Json))
	if err != nil {
		t.Fatal(err)
	}
	for _, quad := range quads {
		fmt.Fprintf(w, "%s\t%s\t%s\t%v\n",
			quad.Subject, quad.Predicate, quad.ObjectId, quad.ObjectVal)
	}
	w.Flush()
}

func Test5(t *testing.T) {
	c := &Case{
		`[
		  {
			"name": "A",
			"age": 25,
			"friends": [
			  {
				"name": "A1",
				"friends": [
				  {
					"name": "A11"
				  },
				  {
					"name": "A12"
				  }
				]
			  },
			 {
				"name": "A2",
				"friends": [
				  {
					"name": "A21"
				  },
				  {
					"name": "A22"
				  }
				]
			  }
			]
		  },
		  {
			"name": "B",
			"age": 26,
			"friends": [
			  {
				"name": "B1",
				"friends": [
				  {
					"name": "B11"
				  },
				  {
					"name": "B12"
				  }
				]
			  },
			 {
				"name": "B2",
				"friends": [
				  {
					"name": "B21"
				  },
				  {
					"name": "B22"
				  }
				]
			  }
			]
		  }
		]`,
		[]Quad{
			{"c.1", "name", "", "A"},
			{"c.1", "age", "", 25},
			{"c.2", "name", "", "A1"},
			{"c.3", "name", "", "A11"},
			{"c.2", "friends", "c.3", nil},
			{"c.4", "name", "", "A12"},
			{"c.2", "friends", "c.4", nil},
			{"c.1", "friends", "c.2", nil},
			{"c.5", "name", "", "A2"},
			{"c.6", "name", "", "A21"},
			{"c.5", "friends", "c.6", nil},
			{"c.7", "name", "", "A22"},
			{"c.5", "friends", "c.7", nil},
			{"c.1", "friends", "c.5", nil},
			{"c.9", "name", "", "B1"},
			{"c.10", "name", "", "B11"},
			{"c.9", "friends", "c.10", nil},
			{"c.11", "name", "", "B12"},
			{"c.9", "friends", "c.11", nil},
			{"c.8", "friends", "c.9", nil},
			{"c.12", "name", "", "B2"},
			{"c.13", "name", "", "B21"},
			{"c.12", "friends", "c.13", nil},
			{"c.14", "name", "", "B22"},
			{"c.12", "friends", "c.14", nil},
			{"c.8", "friends", "c.12", nil},
			{"c.8", "name", "", "B"},
			{"c.8", "age", "", 26},
		},
	}

	quads, err := Parse([]byte(c.Json))
	if err != nil {
		t.Fatal(err)
	}
	if len(quads) != len(c.Quad) {
		t.Fatal("quads returned are incorrect")
	}
}
