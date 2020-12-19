package chunker

import (
	"errors"

	json "github.com/minio/simdjson-go"
)

type FacetType uint8

type Facet struct {
	Key    string
	Value  []byte
	Type   FacetType
	Tokens []string
	Alias  string
}

/*
type Quad struct {
	Subject   string
	Predicate string
	ObjectId  string
	ObjectVal interface{}
	Label     string
	Facets    []*Facet
}
*/

type Quad struct {
	Key   string
	Value interface{}
}

func isScalar(t json.Tag) bool {
	return t != json.TagObjectStart && t != json.TagObjectEnd &&
		t != json.TagArrayStart && t != json.TagArrayEnd
}

func Parse(d []byte) ([]*Quad, error) {
	if !json.SupportedCPU() {
		return nil, errors.New("fast json parsing not supported")
	}
	if len(d) == 0 {
		return nil, nil
	}

	q := make([]*Quad, 0)

	p, err := json.Parse(d, nil)
	if err != nil {
		return nil, err
	}
	i := p.Iter()

	open := false
	quad := &Quad{}

	for t := i.AdvanceInto(); ; t = i.AdvanceInto() {
		switch t {
		case json.TagString:
			if !open {
				if isScalar(i.PeekNextTag()) {
					open = true
					quad.Key, _ = i.String()
				}
			} else {
				open = false
				quad.Value, _ = i.String()
				q = append(q, quad)
				quad = &Quad{}
			}
		case json.TagInteger:
		case json.TagUint:
		case json.TagFloat:
		case json.TagNull:
		case json.TagBoolTrue:
		case json.TagBoolFalse:
		case json.TagObjectStart:
		case json.TagObjectEnd:
		case json.TagArrayStart:
		case json.TagArrayEnd:
		case json.TagRoot:
		case json.TagEnd:
			goto done
		}
	}

done:
	return q, nil
}
