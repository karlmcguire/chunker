package chunker

import (
	"testing"

	"github.com/davecgh/go-spew/spew"
)

func Test(t *testing.T) {
	p := NewParser()
	if err := p.Run([]byte(`{
		"name": "karl"
	}`)); err != nil {
		t.Fatal(err)
	}
	spew.Dump(p.Quads)
}

func Benchmark(b *testing.B) {
	d := []byte(`{
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
		"event_id":""
	}`)

	b.SetBytes(125)
	for n := 0; n < b.N; n++ {
		NewParser().Run(d)
	}
}
