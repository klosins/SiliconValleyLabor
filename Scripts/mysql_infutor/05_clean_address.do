version 15.0
clear all
cap log close
set more off

*******************************************************
* 5. clean address files 
*******************************************************

local inpath "/media/zqian/Seagate Backup Plus Drive/CRD3/address_csv"
local outpath "/media/zqian/Seagate Backup Plus Drive/CRD3/address_csv"

// check if address file exists
cap confirm file "`outpath'/DI_address_cleaned.csv"

// administer cleaning if address file doesn't exist
if _rc != 0 {

* infutor migration data
import delimited "`inpath'/DI_address.csv", clear

sort pid date_eff
lab var addnum "address number for given pid"
lab var add "address - complete address"
lab var add_stnum "address - street number"
lab var add_stpre "address - street prefix"
lab var add_stname "address - street name"
lab var add_sttype "address - street type"
lab var add_stsuf "address - street suffix"
lab var add_aptnum "address - apartment number"
lab var add_city "address - city"
lab var add_state "address - state"
lab var add_zip "address - zip 5-digit"
lab var add_fips "address - fips county code"
lab var date_eff "address - effective date"
lab var date_beg "address - beginning date"
cap lab var date_end "address - end date"
lab var add_id "address - id"

* create address without address number
gen add_noaptnum = add_stnum + " " + add_stpre + " " + add_stname + ///
					" " + add_sttype + " " + add_stsuf
replace add_noaptnum = trim(add_noaptnum)
replace add_noaptnum = itrim(add_noaptnum)
lab var add_noaptnum "address - street address wo apt"

* fix PO Box number order
replace add_noaptnum = add_stname + " " + add_stnum if regexm(add_stname,"PO BOX")

* change zip code to text format
tostring add_zip, format(%05.0f) replace

order pid addnum add_id date_eff date_beg date_end add* 

* convert dates to months
tostring date*, replace
gen month_eff = mofd(date(date_eff,"YM"))
gen month_beg = mofd(date(date_beg,"YM"))
gen month_end = mofd(date(date_end,"YM"))
format %tm month*

* drop addresses with missing effective dates - big drop!!
drop if mi(month_eff)

* create earliest observed date
egen first_seen_temp = rowmin(month*)
bys pid: egen first_seen = min(first_seen_temp)
drop first_seen_temp

* create latest observed date
egen last_seen_temp = rowmax(month*)
bys pid: egen last_seen = max(last_seen_temp)
drop last_seen_temp

* addnum is inaccurate - some effective dates that are out of order
* gen new variable called inorder which is the correct order
gsort pid -month_eff addnum add_id 
bys pid: gen inorder = _n
// assert addnum==inorder // effective dates are not in order

* analyze duplicate pid month_eff
duplicates tag pid month_eff, gen(dup)

* tiebreak for duplicates: same person, same effective month
* keep lowest addnum (latest)
* take the address with the smallest address number (latest)
bys pid month_eff: egen addnum_min = min(addnum)
gen drop = (dup!=0 & addnum_min!=addnum)
drop if drop == 1
drop drop dup addnum_*

* gen new order
drop inorder
gsort pid -month_eff
bys pid: gen inorder = _n

* gen date ranges for each address
sort pid inorder
gen addmonth_beg = month_eff
bys pid: gen addmonth_end = month_eff[_n-1]
bys pid: gen num_pid = _n
replace addmonth_end = last_seen if num_pid==1
format %tm addmonth*
assert addmonth_beg <= addmonth_end // beginning and end dates are consistent

* tag if beginning and end month are the same 
* month is same because it's the latest record and we don't know end date
* we impute an end date of June 2017 (month = 689)
gen bad_last_add = (addmonth_beg == addmonth_end & inorder == 1)
replace addmonth_end = 689 if bad_last_add == 1

* rename
ren inorder inorder_old
assert addmonth_end == last_seen if inorder == 1 & bad_last_add==0 // data is consistent

* collapse effective date when same property is entered several times in a row
forval i=1/15 { 											// loop over all instances
	// dis `i'
	if (`i' == 15) { 										// exit loop if maxval too constraining
		dis "ERROR: Must increase max value of for loop to collapse effective date"
		exit
	}
	sort pid inorder_old add_id
	bys pid: gen double add_id_shift = add_id[_n+1] 		// compare property id to observation before it in time
	bys pid: gen match_prev = (add_id == add_id_shift) 		// tag instances where 2 property id's in adjacent rows are identical
	bys pid: gen match_prev_shift = match_prev[_n-1] 		// shift once
	replace match_prev_shift = 0 if mi(match_prev_shift)	// replace shift values with 0
	bys pid: gen addmonth_beg_shift = addmonth_beg[_n+1] if match_prev == 1 & match_prev_shift == 0 // gen shifted month
	gen addmonth_beg_new = addmonth_beg 					// duplicate addmonth_beg variable
	replace addmonth_beg_new = addmonth_beg_shift if !mi(addmonth_beg_shift)	// replace with new shifted month
	
	count 													// check and store # of obs before drop
	local count_before = r(N)
	
	drop if addmonth_beg == addmonth_beg_shift[_n-1] 		// drop the rows that we replaced
	
	count 													// check and store # of obs after drop
	local count_after = r(N)
	
	ren addmonth_beg addmonth_beg_old`i' 
	ren addmonth_beg_new addmonth_beg
	gsort pid -addmonth_beg
	bys pid: gen inorder_`i' = _n 							// gen new order
	drop add_id_shift match_prev match_prev_shift addmonth_beg_shift 	// drop new vars
	
	if `count_before' == `count_after' {
		gen inorder = inorder_`i'
		continue, break 									// exit loop if no obs were dropped
	}
}

* run checks
assert addmonth_beg < addmonth_end
unique pid inorder 						// should be unique
duplicates report pid addmonth_beg 		// should have no duplicates
local num_obs = r(unique_value)
count
loc num_obs2 = r(N)
assert `num_obs'==`num_obs2' 			// check equality

* clean up data
lab var 	addnum 		    "address number"		
lab var 	addmonth_beg 	"beginning month for address" 
lab var 	addmonth_end 	"end month for address"
lab var 	first_seen		"month where person is first observed"
lab var 	last_seen 		"month where person is last observed"
lab var 	bad_last_add 	"=1 if last address is bad; we impute end date"

format %tm addmonth* *_seen
drop inorder_* addmonth_beg_* date* num_pid month_eff month_beg month_end
order pid inorder addmonth_beg addmonth_end first_seen last_seen

* defines new address number
drop addnum
rename inorder addnum

export delimited using "`outpath'/DI_address_cleaned.csv", replace
}


