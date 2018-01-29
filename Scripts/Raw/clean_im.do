/*
Rose Wang Tan
Franklin Qian

file created: 073017
last edited: 012918

Description: clean Infutor migration data for all states

Steps to run the script:
- ssc install confirmdir
- specify inpath for raw infutor data in txt format
- specify outpath for storing infutor data in dta format:
	- code automatically generates subdirs `outpath'/name, `outpath'/name and `outpath'/address 

ISSUES:
- first and last name need further cleaning: PID is not always correct because of misspellings

*/
version 15.0
clear all
cap log close
set more off

*******************************************************
* 1. convert text files to Stata
*******************************************************

* convert address histories from text to stata format
* only need to do this once!
local inpath "/media/zqian/Seagate Backup Plus Drive/infutor_1perc/data/raw"
local outpath "/media/zqian/Seagate Backup Plus Drive/infutor_1perc/data/dta"

cap confirmdir "`outpath'"
if `r(confirmdir)' != 0 {
	mkdir "`outpath'"
}

local files : dir "`inpath'" files "*"  // store all file names in folder

foreach file in `files' {
	import delimited "`inpath'/`file'", varnames(nonames) clear 
	local file: subinstr local file ".txt" "" // remove .txt from macro string
	
	//rename variables
	cap ren v1 pid
	lab var pid "infutor person id"
	cap ren v2 ssn_primary
	lab var ssn_primary "SSN primary"
	cap ren v3 ssn_primary_seq
	lab var ssn_primary_seq "SSN primary sequence number"
	cap ren v4 ssn_second
	lab var ssn_second "SSN secondary"
	cap ren v5 name_prefix
	lab var name_prefix "name - prefix"
	cap ren v6 name_first 
	lab var name_first "name - first name"
	cap ren v7 name_middle
	lab var name_middle "name - middle initial"
	cap ren v8 name_last 
	lab var name_last "name - last name"
	cap ren v9 name_suffix
	lab var name_suffix "name - suffix"
	cap ren v10 gender
	lab var gender "name - gender"
	cap ren v11 dob
	lab var dob "name - date of birth"
	cap ren v12	deceased
	cap lab var deceased "flag = 1 if deceased person"
	cap ren v18 date_orig  
	lab var date_orig "original file date"
	cap ren v19 date_last 
	lab var date_last "last activity date"

	forval num = 1/10 {	// number of addresses; 10 for most files
	
		loc n=23+(`num'-1)*7
		cap ren v`n' alias`num'
		lab var alias`num' "alias `num'"
		cap tostring alias`num', replace
		cap replace alias`num' = "" if alias`num' == "."
		
		loc n=24+(`num'-1)*7
		cap ren v`n' alias_prefix`num'
		lab var alias_prefix`num' "alias `num' - prefix"
		cap tostring alias_prefix`num', replace
		cap replace alias_prefix`num' = "" if alias_prefix`num' == "."
		
		loc n=25+(`num'-1)*7
		cap ren v`n' alias_first`num'
		lab var alias_first`num' "alias `num' - first"
		cap tostring alias_first`num', replace
		cap replace alias_first`num' = "" if alias_first`num' == "."
		
		loc n=26+(`num'-1)*7
		cap ren v`n' alias_middle`num'
		lab var alias_middle`num' "alias `num' - middle"
		cap tostring alias_middle`num', replace
		cap replace alias_middle`num' = "" if alias_middle`num' == "."
		
		loc n=27+(`num'-1)*7
		cap ren v`n' alias_last`num'
		lab var alias_last`num' "alias `num' - last"
		cap tostring alias_last`num', replace
		cap replace alias_last`num' = "" if alias_last`num' == "."
		
		loc n=28+(`num'-1)*7
		cap ren v`n' alias_suffix`num'
		lab var alias_suffix`num' "alias `num' - suffix"
		cap tostring alias_suffix`num', replace
		cap replace alias_suffix`num' = "" if alias_suffix`num' == "."
		
		loc n=29+(`num'-1)*7
		cap ren v`n' alias_gender`num'
		lab var alias_gender`num' "alias `num' - gender"
		cap tostring alias_gender`num', replace
		cap replace alias_gender`num' = "" if alias_gender`num' == "."
	
		loc n=94+(`num'-1)*25
		cap ren v`n' add`num'
		lab var add`num' "address `num' - complete address"
		
		loc n=96+(`num'-1)*25
		cap ren v`n' add_stnum`num'
		lab var add_stnum`num' "address `num' - street number"
		cap tostring add_stnum`num', replace
		
		loc n=97+(`num'-1)*25
		cap ren v`n' add_stpre`num'
		lab var add_stpre`num' "address `num' - street prefix"
		cap tostring add_stpre`num', replace
		cap replace add_stpre`num' = "" if add_stpre`num' == "."
		
		loc n=98+(`num'-1)*25
		cap ren v`n' add_stname`num'
		lab var add_stname`num' "address `num' - street name"
				
		loc n=99+(`num'-1)*25
		cap ren v`n' add_sttype`num'
		lab var add_sttype`num' "address `num' - street type"
				
		loc n=100+(`num'-1)*25
		cap ren v`n' add_stsuf`num'
		lab var add_stsuf`num' "address `num' - street suffix"
		cap tostring add_stsuf`num', replace
		cap replace add_stsuf`num' = "" if add_stsuf`num' == "."

		loc n=102+(`num'-1)*25
		cap ren v`n' add_aptnum`num'
		lab var add_aptnum`num' "address `num' - apartment number"
		
		loc n=103+(`num'-1)*25
		cap ren v`n' add_city`num'
		lab var add_city`num' "address `num' - city"
		
		loc n=104+(`num'-1)*25
		cap ren v`n' add_state`num'
		lab var add_state`num' "address `num' - state"
		
		loc n=105+(`num'-1)*25	
		cap ren v`n' add_zip`num'
		lab var add_zip`num' "address `num' - zip 5-digit"
		
		loc n=110+(`num'-1)*25	
		cap ren v`n' add_fips`num'
		lab var add_fips`num' "address `num' - fips county code"	
		
		loc n=113+(`num'-1)*25
		cap ren v`n' date_eff`num'
		lab var date_eff`num' "address `num' - effective date"
		
		loc n=114+(`num'-1)*25
		cap ren v`n' date_beg`num'
		cap lab var date_beg`num' "address `num' - beginning date"
		
		loc n=117+(`num'-1)*25
		cap ren v`n' date_end`num'
		lab var date_end`num' "address `num' - end date"
		
		loc n=115+(`num'-1)*25
		cap ren v`n' add_id`num'
		lab var add_id`num' "address `num' - id"
		
		loc n=343+(`num'-1)*7
		cap ren v`n' phone`num'
		lab var phone`num' "phone `num'"
		cap tostring phone`num', replace
		cap replace phone`num' = "" if phone`num' == "."
		
		loc n=344+(`num'-1)*7
		cap ren v`n' phone_internal1`num'
		lab var phone_internal1`num' "phone `num' - internal use 1"
		cap tostring phone_internal1`num', replace
		cap replace phone_internal1`num' = "" if phone_internal1`num' == "."
		
		loc n=345+(`num'-1)*7
		cap ren v`n' phone_internal2`num'
		lab var phone_internal2`num' "phone `num' - internal use 2"
		
		loc n=346+(`num'-1)*7
		cap ren v`n' phone_internal3`num'
		lab var phone_internal3`num' "phone `num' - internal use 3"
		cap tostring phone_internal3`num', replace
		cap replace phone_internal3`num' = "" if phone_internal3`num' == "."
		
		loc n=347+(`num'-1)*7
		cap ren v`n' phone_internal4`num'
		lab var phone_internal4`num' "phone `num' - internal use 4"
		cap tostring phone_internal4`num', replace
		cap replace phone_internal4`num' = "" if phone_internal4`num' == "."
		
		loc n=348+(`num'-1)*7
		cap ren v`n' phone_date_orig`num'
		lab var phone_date_orig`num' "phone `num' - begin date"
		
		loc n=349+(`num'-1)*7
		cap ren v`n' phone_date_last`num'
		lab var phone_date_last`num' "phone `num' - end date"
	}
	
	save "`outpath'/`file'.dta", replace
}

*******************************************************
* 2. clean name files 
*******************************************************
local files : dir "`outpath'" files "*.dta"  // store all file names in folder

cap confirmdir "`outpath'/name"
if `r(confirmdir)' != 0 {
	mkdir "`outpath'/name"
}

foreach file in `files' {

	local file_sub: subinstr local file "CRD3_" "" // remove CRD3_ from macro string
	local file_sub: subinstr local file_sub ".dta" "" // remove .dta from macro string
	dis "processing: `file_sub'"
	
	quietly {
	// check if address file exists
	cap confirm file "`outpath'/name/`file_sub'_name.dta"
	
	// administer cleaning if address file doesn't exist
	if _rc != 0 {
	
	// infutor migration data
	use "`outpath'/`file'", clear
	
	// dead people
	gen dead = .
	cap replace dead = 1 if deceased == "Y"
	lab var dead	"=1 if person deceased, may not always be true"

	keep pid ssn* name* gender dob dead alias*
	reshape long alias alias_prefix alias_first alias_middle alias_last ///
		alias_suffix alias_gender, i(pid) j(alias_num)
	
	// drop missings
	drop if mi(alias)
	
	compress
	datasignature set, reset
	notes: 		`file_sub'_name.dta 					/	///
				cleaned name data - `file_sub'				/	///
				clean_im.do / TS
	label 		data "cleaned name data - `file_sub' / `c(current_date)'"
	save 		"`outpath'/name/`file_sub'_name.dta", 		replace
	}
	}
}

*******************************************************
* 3. clean telephone files 
*******************************************************
local files : dir "`outpath'" files "*.dta"  // store all file names in folder

cap confirmdir "`outpath'/phone"
if `r(confirmdir)' != 0 {
	mkdir "`outpath'/phone"
}

foreach file in `files' {

	local file_sub: subinstr local file "CRD3_" "" // remove CRD3_ from macro string
	local file_sub: subinstr local file_sub ".dta" "" // remove .dta from macro string
	dis "processing: `file_sub'"
	
	quietly {
	// check if address file exists
	cap confirm file "`outpath'/phone/`file_sub'_phone.dta"
	
	// administer cleaning if address file doesn't exist
	if _rc != 0 {
	
	// infutor migration data
	use "`outpath'/`file'", clear
	
	// dead people
	gen dead = .
	cap replace dead = 1 if deceased == "Y"
	lab var dead	"=1 if person deceased, may not always be true"

	keep pid ssn* name* gender dob dead phone*
	reshape long phone phone_internal1 phone_internal2 phone_internal3 ///
		phone_internal4 phone_date_orig phone_date_last, i(pid) j(phone_num)
		
	tostring phone_date_orig phone_date_last, replace
	gen phone_month_orig = mofd(date(phone_date_orig,"YM")) if length(phone_date_orig)==6
	replace phone_month_orig = mofd(date(phone_date_orig,"YMD")) if length(phone_date_orig)>6
	gen phone_month_last = mofd(date(phone_date_last,"YM")) if length(phone_date_last)==6
	replace phone_month_last = mofd(date(phone_date_last,"YMD")) if length(phone_date_last)>6
	format %tm phone_month_*
	drop phone_date_orig phone_date_last
	
	// drop missings
	drop if mi(phone)
	
	compress
	datasignature set, reset
	notes: 		`file_sub'_phone.dta 					/	///
				cleaned phone data - `file_sub'				/	///
				clean_im.do / TS
	label 		data "cleaned phone data - `file_sub' / `c(current_date)'"
	save 		"`outpath'/phone/`file_sub'_phone.dta", 		replace
	}
	}
}

*******************************************************
* 4. clean address files 
*******************************************************
local files : dir "`outpath'" files "*.dta"  // store all file names in folder

cap confirmdir "`outpath'/address"
if `r(confirmdir)' != 0 {
	mkdir "`outpath'/address"
}

foreach file in `files' {

	local file_sub: subinstr local file "CRD3_" "" // remove CRD3_ from macro string
	local file_sub: subinstr local file_sub ".dta" "" // remove .dta from macro string
	dis "processing:  `file_sub'"
	
	quietly {
	// check if address file exists
	cap confirm file "`outpath'/address/`file_sub'_address.dta"
	
	// administer cleaning if address file doesn't exist
	if _rc != 0 {
	
	* infutor migration data
	use "`outpath'/`file'", clear

	keep pid ssn* date* add* name* gender dob deceased date_orig date_last
	reshape long add_id date_eff date_beg date_end add add_stnum add_stpre ///
				add_stname add_sttype add_stsuf add_aptnum add_city ///
				add_state add_zip add_fips, i(pid) j(addnum)

	* drop missings
	drop if mi(add_id)
	
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
	
	order pid ssn_* addnum add_id date_eff date_beg date_end date_ori date_last add* 
	
	* convert dates to months
	tostring date*, replace
	gen month_eff = mofd(date(date_eff,"YM"))
	gen month_beg = mofd(date(date_beg,"YM"))
	gen month_end = mofd(date(date_end,"YM"))
	gen month_orig = mofd(date(date_orig,"YMD"))
	gen month_last = mofd(date(date_last,"YMD"))
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
	
	* dead people
	gen dead = .
	cap replace dead = 1 if deceased == "Y"
	
	* clean up data
	lab var 	addnum 		    "address number"		
	lab var 	addmonth_beg 	"beginning month for address" 
	lab var 	addmonth_end 	"end month for address"
	lab var 	first_seen		"month where person is first observed"
	lab var 	last_seen 		"month where person is last observed"
	lab var 	bad_last_add 	"=1 if last address is bad; we impute end date"
	lab var         dead		"=1 if person deceased, may not always be true"
	
	format %tm addmonth* *_seen
	drop inorder_* addmonth_beg_* date* num_pid month_eff month_beg month_end ///
			month_orig month_last deceased
	order pid inorder addmonth_beg addmonth_end first_seen last_seen

	* defines new address number
	drop addnum
	rename inorder addnum

	compress
	datasignature set, reset
	notes: 		`file_sub'_address.dta 					/	///
				cleaned address data - `file_sub'				/	///
				clean_im.do / TS
	label 		data "cleaned address data - `file_sub' / `c(current_date)'"
	save 		"`outpath'/address/`file_sub'_address.dta", 		replace
	}
	} // end quietly
} // end for loop
