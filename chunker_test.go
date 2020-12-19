package chunker

import (
	"testing"

	"github.com/davecgh/go-spew/spew"
)

func Test(t *testing.T) {
	spew.Dump(Parse([]byte(`{
		"name": "Karl",
		"wow!": "nskdfjnksjfdn",
		"woooops": {},
		"more": [{
			"one": "1",
			"two": "2",
			"three": "3"
		}, {
			"something": "else",
			"this": "is nice!!!!",
			"miss": {}
		}]
	}`)))
}
