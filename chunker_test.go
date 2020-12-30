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

func BenchmarkFillerQuads(b *testing.B) {
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
		Parse(json)
	}
}
