package chunker

import (
	"fmt"
	"os"
	"testing"
	"text/tabwriter"
)

func Test1(t *testing.T) {
	json := []byte(`{
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
	}`)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "subject\tpredicate\tobject_id\tobject_val\n")
	defer w.Flush()

	quads, err := NewParser().Parse(json)
	if err != nil {
		t.Fatal(err)
	}

	for _, quad := range quads {
		fmt.Fprintf(w, "%s\t%s\t%s\t%v\n",
			quad.Subject, quad.Predicate, quad.ObjectId, quad.ObjectVal)
	}
}
