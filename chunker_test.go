package chunker

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
)

type Case struct {
	Json      []byte
	Quads     []*Quad
	ExpectErr bool
}

func (c *Case) Test(t *testing.T, logs bool) {
	quads, err := NewParser(logs).Parse(c.Json)
	if err != nil {
		if c.ExpectErr {
			return
		}
		t.Fatal(err)
	}
	if c.ExpectErr {
		t.Fatalf("expected an error")
	}
	if len(quads) != len(c.Quads) {
		t.Fatalf("expected %d quads but got %d\n", len(c.Quads), len(quads))
	}
	for i, quad := range quads {
		if quad.Subject != c.Quads[i].Subject {
			spew.Dump(quad)
			t.Fatalf("expected '%s' subject for quad %d but got '%s'\n",
				c.Quads[i].Subject, i, quad.Subject)
		}
		if quad.Predicate != c.Quads[i].Predicate {
			spew.Dump(quad)
			t.Fatalf("expected '%s' predicate for quad %d but got '%s'\n",
				c.Quads[i].Predicate, i, quad.Predicate)
		}
		if quad.ObjectId != c.Quads[i].ObjectId {
			spew.Dump(quad)
			t.Fatalf("expected '%s' objectId for quad %d but got '%s'\n",
				c.Quads[i].ObjectId, i, quad.ObjectId)
		}
		if quad.ObjectVal == nil && c.Quads[i].ObjectVal == nil {
			continue
		}
		// make sure the values are of the same type
		valType := reflect.TypeOf(quad.ObjectVal).String()
		correctValType := reflect.TypeOf(c.Quads[i].ObjectVal).String()
		if valType != correctValType {
			spew.Dump(quad)
			t.Fatalf("expected %s objectVal for quad %d but got %s\n",
				correctValType, i, valType)
		}
		// make sure the values are equal
		switch correctValType {
		case "string":
			if quad.ObjectVal.(string) != c.Quads[i].ObjectVal.(string) {
				t.Fatalf("expected '%s' objectVal for quad %d but got '%s'\n",
					c.Quads[i].ObjectVal.(string), i, quad.ObjectVal.(string))
			}
		case "int64":
			if quad.ObjectVal.(int64) != c.Quads[i].ObjectVal.(int64) {
				t.Fatalf("expected %d objectVal for quad %d but got %d\n",
					c.Quads[i].ObjectVal.(int64), i, quad.ObjectVal.(int64))
			}
		case "uint64":
			if quad.ObjectVal.(uint64) != c.Quads[i].ObjectVal.(uint64) {
				t.Fatalf("expected %d objectVal for quad %d but got %d\n",
					c.Quads[i].ObjectVal.(uint64), i, quad.ObjectVal.(uint64))
			}
		case "float64":
			if quad.ObjectVal.(float64) != c.Quads[i].ObjectVal.(float64) {
				t.Fatalf("expected %f objectVal for quad %s but got %f\n",
					c.Quads[i].ObjectVal.(float64), quad.Subject, quad.ObjectVal.(float64))
			}
		case "bool":
			if quad.ObjectVal.(bool) != c.Quads[i].ObjectVal.(bool) {
				t.Fatalf("expected %v objectVal for quad %d got %v\n",
					c.Quads[i].ObjectVal.(bool), i, quad.ObjectVal.(bool))
			}
		default:
			t.Fatal("objectVal type not handled")
		}
		// check facets
		if len(c.Quads[i].Facets) > 0 {
			if quad.Facets == nil || len(quad.Facets) == 0 {
				t.Fatalf("expected facets for quad %d, but found none\n", i)
			}
			for j, facet := range quad.Facets {
				if facet.ValType != c.Quads[i].Facets[j].ValType {
					spew.Dump(facet)
					spew.Dump(c.Quads[i].Facets[j])
					t.Fatalf("expected %s valType for quad %d facet %d but got %s\n",
						c.Quads[i].Facets[j].ValType.String(), i, j, facet.ValType.String())
				}
				if !bytes.Equal(facet.Value, c.Quads[i].Facets[j].Value) {
					spew.Dump(facet)
					t.Fatalf("expected %v value for quad %d facet %d but got %v\n",
						c.Quads[i].Facets[j].Value, i, j, facet.Value)
				}
			}
		}
	}
}

// simdjson has number parsing issues, so this is a very important test
func TestNumbers(t *testing.T) {
	cases := []*Case{
		{
			Json: []byte(`{
						"uid": "1",
						"key": 9223372036854775299
					}`),
			Quads: []*Quad{{
				Subject:   "1",
				Predicate: "key",
				ObjectVal: int64(9223372036854775299),
			}},
		},
		{
			Json: []byte(`{
						"uid": "2",
						"key": 9223372036854775299.0
					}`),
			Quads: []*Quad{{
				Subject:   "2",
				Predicate: "key",
				ObjectVal: float64(9223372036854775299.0),
			}},
		},
		{
			Json: []byte(`{
				"uid": "3",
				"key": 27670116110564327426
			}`),
			ExpectErr: true,
		},
		{
			Json: []byte(`{
						"uid": "4",
						"key": "23452786"
					}`),
			Quads: []*Quad{{
				Subject:   "4",
				Predicate: "key",
				ObjectVal: "23452786",
			}},
		},
		{
			Json: []byte(`{
						"uid": "5",
						"key": "23452786.2378"
					}`),
			Quads: []*Quad{{
				Subject:   "5",
				Predicate: "key",
				ObjectVal: "23452786.2378",
			}},
		},
		{
			Json: []byte(`{
						"uid": "6",
						"key": -1e10
					}`),
			Quads: []*Quad{{
				Subject:   "6",
				Predicate: "key",
				ObjectVal: float64(-1e+10),
			}},
		},
		{
			Json: []byte(`{
						"uid": "7",
						"key": 0E-0
					}`),
			Quads: []*Quad{{
				Subject:   "7",
				Predicate: "key",
				ObjectVal: float64(0),
			}},
		},
	}
	for _, c := range cases {
		c.Test(t, false)
	}
}

// TODO
func TestFacets1(t *testing.T) {
	/*
		TODO: this is the correct output, the case should match this:

		predicate:"mobile" object_value:<str_val:"040123456" >

		    facets:<key:"operation"
					value:"READ WRITE"
					tokens:"\001read"
					tokens:"\001write" > )


		predicate:"car" object_value:<str_val:"MA0123" >

		    facets:<key:"age"
					value:"\003\000\000\000\000\000\000\000"
					val_type:INT >

		    facets:<key:"price"
					value:"q=\n\327#L\335@"
					val_type:FLOAT >

		    facets:<key:"since"
					value:"\001\000\000\000\016\273K7\345\000\000\000\000\377\377"
					val_type:DATETIME >

		    facets:<key:"first" value:"\001" val_type:BOOL > ),


		predicate:"name" object_value:<str_val:"Alice" > )
	*/

	c := &Case{
		Json: []byte(`[{
			"name": "Alice",
			"mobile": "040123456",
			"car": "MA0123",
			"mobile|operation": "READ WRITE",
			"car|first": true,
			"car|age": 3,
			"car|price": 30000.56,
			"car|since": "2006-01-02T15:04:05Z"
		}]`),
		Quads: []*Quad{{
			Subject:   "c.1",
			Predicate: "name",
			ObjectId:  "",
			ObjectVal: "Alice",
		}, {
			Subject:   "c.1",
			Predicate: "mobile",
			ObjectId:  "",
			ObjectVal: "040123456",
			Facets: []*Facet{{
				Key:     "operation",
				Value:   []byte(`READ WRITE`),
				ValType: STRING,
			}},
		}, {
			Subject:   "c.1",
			Predicate: "car",
			ObjectId:  "",
			ObjectVal: "MA0123",
			Facets: []*Facet{{
				Key:     "first",
				Value:   []byte{0x01},
				ValType: BOOL,
			}, {
				Key: "age",
				Value: []byte{
					0x03, 0x00, 0x00, 0x00,
					0x00, 0x00, 0x00, 0x00},
				ValType: INT,
			}, {
				Key: "price",
				Value: []byte{
					0x71, 0x3d, 0x0a, 0xd7,
					0x23, 0x4c, 0xdd, 0x40,
				},
				ValType: FLOAT,
			}, {
				Key: "since",
				Value: []byte{
					0x01, 0x00, 0x00, 0x00,
					0x0e, 0xbb, 0x4b, 0x37,
					0xe5, 0x00, 0x00, 0x00,
					0x00, 0xff, 0xff},
				ValType: DATETIME,
			}},
		}},
	}
	c.Test(t, false)
}

func Test1(t *testing.T) {
	c := &Case{
		Json: []byte(`{
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
		}`),
		Quads: []*Quad{{
			Subject:   "c.1",
			Predicate: "name",
			ObjectId:  "",
			ObjectVal: "Alice",
		}, {
			Subject:   "c.2",
			Predicate: "name",
			ObjectId:  "",
			ObjectVal: "Charlie",
		}, {
			Subject:   "c.2",
			Predicate: "married",
			ObjectId:  "",
			ObjectVal: false,
		}, {
			Subject:   "1000",
			Predicate: "name",
			ObjectId:  "",
			ObjectVal: "Bob",
		}, {
			Subject:   "c.1",
			Predicate: "friend",
			ObjectId:  "c.2",
			ObjectVal: nil,
		}, {
			Subject:   "c.1",
			Predicate: "friend",
			ObjectId:  "1000",
			ObjectVal: nil,
		}},
	}
	c.Test(t, false)
}

func Test2(t *testing.T) {
	c := &Case{
		Json: []byte(`{
			"name": "Alice",
			"address": {},
			"school": {
				"name": "Wellington Public School"
			}
		}`),
		Quads: []*Quad{{
			Subject:   "c.1",
			Predicate: "name",
			ObjectId:  "",
			ObjectVal: "Alice",
		}, {
			Subject:   "c.2",
			Predicate: "name",
			ObjectId:  "",
			ObjectVal: "Wellington Public School",
		}, {
			Subject:   "c.1",
			Predicate: "school",
			ObjectId:  "c.2",
			ObjectVal: nil,
		}},
	}
	c.Test(t, false)
}

func Test3(t *testing.T) {
	c := &Case{
		Json: []byte(`[
			{
				"name": "Alice",
				"mobile": "040123456",
				"car": "MA0123", 
				"age": 21, 
				"weight": 58.7
			}
		]`),
		Quads: []*Quad{{
			Subject:   "c.1",
			Predicate: "name",
			ObjectId:  "",
			ObjectVal: "Alice",
		}, {
			Subject:   "c.1",
			Predicate: "mobile",
			ObjectId:  "",
			ObjectVal: "040123456",
		}, {
			Subject:   "c.1",
			Predicate: "car",
			ObjectId:  "",
			ObjectVal: "MA0123",
		}, {
			Subject:   "c.1",
			Predicate: "age",
			ObjectId:  "",
			ObjectVal: int64(21),
		}, {
			Subject:   "c.1",
			Predicate: "weight",
			ObjectId:  "",
			ObjectVal: float64(58.7),
		}},
	}
	c.Test(t, false)
}

func Test4(t *testing.T) {
	c := &Case{
		Json: []byte(`{
			"name": "Alice",
			"age": 25,
			"friends": [
				{
					"name": "Bob"
				}
			]	
		}`),
		Quads: []*Quad{{
			Subject:   "c.1",
			Predicate: "name",
			ObjectId:  "",
			ObjectVal: "Alice",
		}, {
			Subject:   "c.1",
			Predicate: "age",
			ObjectId:  "",
			ObjectVal: int64(25),
		}, {
			Subject:   "c.2",
			Predicate: "name",
			ObjectId:  "",
			ObjectVal: "Bob",
		}, {
			Subject:   "c.1",
			Predicate: "friends",
			ObjectId:  "c.2",
			ObjectVal: nil,
		}},
	}
	c.Test(t, false)
}

func Test5(t *testing.T) {
	c := &Case{
		Json: []byte(`[
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
		]`),
		Quads: []*Quad{
			{"c.1", "name", "", "A", nil},
			{"c.1", "age", "", int64(25), nil},
			{"c.2", "name", "", "A1", nil},
			{"c.3", "name", "", "A11", nil},
			{"c.4", "name", "", "A12", nil},
			{"c.2", "friends", "c.3", nil, nil},
			{"c.2", "friends", "c.4", nil, nil},
			{"c.5", "name", "", "A2", nil},
			{"c.6", "name", "", "A21", nil},
			{"c.7", "name", "", "A22", nil},
			{"c.5", "friends", "c.6", nil, nil},
			{"c.5", "friends", "c.7", nil, nil},
			{"c.1", "friends", "c.2", nil, nil},
			{"c.1", "friends", "c.5", nil, nil},
			{"c.8", "name", "", "B", nil},
			{"c.8", "age", "", int64(26), nil},
			{"c.9", "name", "", "B1", nil},
			{"c.10", "name", "", "B11", nil},
			{"c.11", "name", "", "B12", nil},
			{"c.9", "friends", "c.10", nil, nil},
			{"c.9", "friends", "c.11", nil, nil},
			{"c.12", "name", "", "B2", nil},
			{"c.13", "name", "", "B21", nil},
			{"c.14", "name", "", "B22", nil},
			{"c.12", "friends", "c.13", nil, nil},
			{"c.12", "friends", "c.14", nil, nil},
			{"c.8", "friends", "c.9", nil, nil},
			{"c.8", "friends", "c.12", nil, nil},
		},
	}
	c.Test(t, false)
}

func TestGeo(t *testing.T) {
	c := &Case{
		Json: []byte(`{
			"name": "Alice",
			"age": 26.3,
			"married": true,
			"now": "2020-12-29T17:39:34.816808024Z",
			"address": {
				"type": "Point",
				"coordinates": [
					1.1,
					2
				]
			}
		}`),
		Quads: []*Quad{{
			Subject:   "c.1",
			Predicate: "name",
			ObjectId:  "",
			ObjectVal: "Alice",
		}, {
			Subject:   "c.1",
			Predicate: "age",
			ObjectId:  "",
			ObjectVal: float64(26.3),
		}, {
			Subject:   "c.1",
			Predicate: "married",
			ObjectId:  "",
			ObjectVal: true,
		}, {
			Subject:   "c.1",
			Predicate: "now",
			ObjectId:  "",
			ObjectVal: "2020-12-29T17:39:34.816808024Z",
		}, {
			Subject:   "c.1",
			Predicate: "address",
			ObjectId:  "",
			ObjectVal: "[1.1 2]",
		}},
	}
	c.Test(t, false)
}

// NOTE: 2.4M nquads/sec on thinkpad x1 carbon with zero allocations--this is
//       probably the upper limit on performance
func Benchmark1(b *testing.B) {
	json := []byte(`[
	{
		"uid":123,
		"flguid":123,
		"is_validate":"xxxxxxxxxx",
		"createDatetime":"xxxxxxxxxx",
		"contains":{
			"createDatetime":"xxxxxxxxxx",
			"final_individ":"xxxxxxxxxx",
			"cm_bad_debt":"xxxxxxxxxx",
			"cm_bill_address1":"xxxxxxxxxx",
			"cm_bill_address2":"xxxxxxxxxx",
			"cm_bill_city":"xxxxxxxxxx",
			"cm_bill_state":"xxxxxxxxxx",
			"cm_zip":"xxxxxxxxxx",
			"zip5":"xxxxxxxxxx",
			"cm_customer_id":"xxxxxxxxxx",
			"final_gaid":"xxxxxxxxxx",
			"final_hholdid":"xxxxxxxxxx",
			"final_firstname":"xxxxxxxxxx",
			"final_middlename":"xxxxxxxxxx",
			"final_surname":"xxxxxxxxxx",
			"final_gender":"xxxxxxxxxx",
			"final_ace_prim_addr":"xxxxxxxxxx",
			"final_ace_sec_addr":"xxxxxxxxxx",
			"final_ace_urb":"xxxxxxxxxx",
			"final_ace_city_llidx":"xxxxxxxxxx",
			"final_ace_state":"xxxxxxxxxx",
			"final_ace_postal_code":"xxxxxxxxxx",
			"final_ace_zip4":"xxxxxxxxxx",
			"final_ace_dpbc":"xxxxxxxxxx",
			"final_ace_checkdigit":"xxxxxxxxxx",
			"final_ace_iso_code":"xxxxxxxxxx",
			"final_ace_cart":"xxxxxxxxxx",
			"final_ace_lot":"xxxxxxxxxx",
			"final_ace_lot_order":"xxxxxxxxxx",
			"final_ace_rec_type":"xxxxxxxxxx",
			"final_ace_remainder":"xxxxxxxxxx",
			"final_ace_dpv_cmra":"xxxxxxxxxx",
			"final_ace_dpv_ftnote":"xxxxxxxxxx",
			"final_ace_dpv_status":"xxxxxxxxxx",
			"final_ace_foreigncode":"xxxxxxxxxx",
			"final_ace_match_5":"xxxxxxxxxx",
			"final_ace_match_9":"xxxxxxxxxx",
			"final_ace_match_un":"xxxxxxxxxx",
			"final_ace_zip_move":"xxxxxxxxxx",
			"final_ace_ziptype":"xxxxxxxxxx",
			"final_ace_congress":"xxxxxxxxxx",
			"final_ace_county":"xxxxxxxxxx",
			"final_ace_countyname":"xxxxxxxxxx",
			"final_ace_factype":"xxxxxxxxxx",
			"final_ace_fipscode":"xxxxxxxxxx",
			"final_ace_error_code":"xxxxxxxxxx",
			"final_ace_stat_code":"xxxxxxxxxx",
			"final_ace_geo_match":"xxxxxxxxxx",
			"final_ace_geo_lat":"xxxxxxxxxx",
			"final_ace_geo_lng":"xxxxxxxxxx",
			"final_ace_ageo_pla":"xxxxxxxxxx",
			"final_ace_geo_blk":"xxxxxxxxxx",
			"final_ace_ageo_mcd":"xxxxxxxxxx",
			"final_ace_cgeo_cbsa":"xxxxxxxxxx",
			"final_ace_cgeo_msa":"xxxxxxxxxx",
			"final_ace_ap_lacscode":"xxxxxxxxxx",
			"final_dsf_businessflag":"xxxxxxxxxx",
			"final_dsf_dropflag":"xxxxxxxxxx",
			"final_dsf_throwbackflag":"xxxxxxxxxx",
			"final_dsf_seasonalflag":"xxxxxxxxxx",
			"final_dsf_vacantflag":"xxxxxxxxxx",
			"final_dsf_deliverytype":"xxxxxxxxxx",
			"final_dsf_dt_curbflag":"xxxxxxxxxx",
			"final_dsf_dt_ndcbuflag":"xxxxxxxxxx",
			"final_dsf_dt_centralflag":"xxxxxxxxxx",
			"final_dsf_dt_doorslotflag":"xxxxxxxxxx",
			"final_dsf_dropcount":"xxxxxxxxxx",
			"final_dsf_nostatflag":"xxxxxxxxxx",
			"final_dsf_educationalflag":"xxxxxxxxxx",
			"final_dsf_rectyp":"xxxxxxxxxx",
			"final_mailability_score":"xxxxxxxxxx",
			"final_occupancy_score":"xxxxxxxxxx",
			"final_multi_type":"xxxxxxxxxx",
			"final_deceased_flag":"xxxxxxxxxx",
			"final_dnm_flag":"xxxxxxxxxx",
			"final_dnc_flag":"xxxxxxxxxx",
			"final_dnf_flag":"xxxxxxxxxx",
			"final_prison_flag":"xxxxxxxxxx",
			"final_nursing_home_flag":"xxxxxxxxxx",
			"final_date_of_birth":"xxxxxxxxxx",
			"final_date_of_death":"xxxxxxxxxx",
			"vip_number":"xxxxxxxxxx",
			"vip_store_no":"xxxxxxxxxx",
			"vip_division":"xxxxxxxxxx",
			"vip_phone_number":"xxxxxxxxxx",
			"vip_email_address":"xxxxxxxxxx",
			"vip_first_name":"xxxxxxxxxx",
			"vip_last_name":"xxxxxxxxxx",
			"vip_gender":"xxxxxxxxxx",
			"vip_status":"xxxxxxxxxx",
			"vip_membership_date":"xxxxxxxxxx",
			"vip_expiration_date":"xxxxxxxxxx",
			"cm_date_addr_chng":"xxxxxxxxxx",
			"cm_date_entered":"xxxxxxxxxx",
			"cm_name":"xxxxxxxxxx",
			"cm_opt_on_acct":"xxxxxxxxxx",
			"cm_origin":"xxxxxxxxxx",
			"cm_orig_acq_source":"xxxxxxxxxx",
			"cm_phone_number":"xxxxxxxxxx",
			"cm_phone_number2":"xxxxxxxxxx",
			"cm_problem_cust":"xxxxxxxxxx",
			"cm_rm_list":"xxxxxxxxxx",
			"cm_rm_rented_list":"xxxxxxxxxx",
			"cm_tax_code":"xxxxxxxxxx",
			"email_address":"xxxxxxxxxx",
			"esp_email_id":"xxxxxxxxxx",
			"esp_sub_date":"xxxxxxxxxx",
			"esp_unsub_date":"xxxxxxxxxx",
			"cm_user_def_1":"xxxxxxxxxx",
			"cm_user_def_7":"xxxxxxxxxx",
			"do_not_phone":"xxxxxxxxxx",
			"company_num":"xxxxxxxxxx",
			"customer_id":"xxxxxxxxxx",
			"load_date":"xxxxxxxxxx",
			"activity_date":"xxxxxxxxxx",
			"email_address_hashed":"xxxxxxxxxx",
			"event_id":"",
			"contains":{
				"uid": 123,
				"flguid": 123,
				"is_validate":"xxxxxxxxxx",
				"createDatetime":"xxxxxxxxxx"
			}
		}
	}]`)

	b.SetBytes(125)
	for n := 0; n < b.N; n++ {
		NewParser(false).Parse(json)
	}
}
