version 15.0
clear all
cap log close
set more off

use ctystate_detail, clear
unique city_key zip
bys city_key: gen count=_N

use city_county_unique.dta, clear
unique county_fips city_key
unique unique_city_key

rename city city_name
merge 1:m county_fips city_key using ctystate_detail, assert(3) keep(3) nogen

unique city_key zip
unique city_name zip

save cityname_std.dta, replace
export delimited using "/home/zqian/Documents/patents/data/cityname/cityname_std.csv", replace

use "/media/zqian/Seagate Backup Plus Drive/infutor_1perc/data/dta/address/MA1perc_address", clear
keep add_city add_state add_zip add_fips
sort add_city add_zip
drop if add_city==""

//standardize cardinal directions
replace add_city = trim(add_city)
replace add_city = " " + add_city + " "
replace add_city = subinstr(add_city, " E ", " EAST ", .) if regex(add_city, " E ")
replace add_city = subinstr(add_city, " W ", " WEST ", .) if regex(add_city, " W ")
replace add_city = subinstr(add_city, " S ", " SOUTH ", .) if regex(add_city, " S ")
replace add_city = subinstr(add_city, " N ", " NORTH ", .) if regex(add_city, " N ")
replace add_city = trim(add_city)

duplicates drop
tostring add_fips, format(%03.0f) replace

unique add_city add_zip add_fips
/*
Number of unique values of add_city add_zip add_fips is  7414
Number of records is  7414
*/

//exact merge based on city name and zip to get preferred name
mmerge add_city add_zip using "/home/zqian/Documents/patents/data/cityname/cityname_std", ///
	type(n:1) unmatched(both) umatch(city_name zip)
	
/*
------------+---------------------------------------------------------
              _merge |    498  obs only in master data                (code==1)
                     |  57687  obs only in using data                 (code==2)
                     |   6916  obs both in master and using data      (code==3)
-------------------------------------------------------------------------------
*/

preserve
keep if _m==3
drop _m
save "/home/zqian/Documents/patents/data/cityname/MA1perc_cityname_match1", replace
restore

keep if _m==1
keep add_city add_state add_fips add_zip
save "/home/zqian/Documents/patents/data/cityname/MA1perc_cityname_unmatch1", replace

use "/home/zqian/Documents/patents/data/cityname/MA1perc_cityname_unmatch1", clear
//drop zip for these cases as there are probably no preferred name based on zipcode
drop add_zip
duplicates drop

unique add_city add_state add_fips
/*
Number of unique values of add_city add_state add_fips is  418
Number of records is  418
*/

mmerge add_city add_state add_fips using "/home/zqian/Documents/patents/data/cityname/cityname_std", ///
	type(1:n) unmatched(both) umatch(city_name state_abbr county_fips)

/*
------------+---------------------------------------------------------
              _merge |    231  obs only in master data                (code==1)
                     |  62527  obs only in using data                 (code==2)
                     |   1961  obs both in master and using data      (code==3)
-------------------------------------------------------------------------------
*/	

keep if _m==3
bys add_city add_state add_fips: egen nmatch=count(zip)
bys add_city add_state add_fips: drop if pref_city_name==pref_city_name[_n-1] & nmatch>0
keep add_city add_state add_fips pref_city_name

unique add_city add_state add_fips
/*
Number of unique values of add_city add_state add_fips is  187
Number of records is  351
*/

mmerge add_city add_state add_fips using "/home/zqian/Documents/patents/data/cityname/MA1perc_cityname_unmatch1", ///
	type(n:n) unmatched(using)
drop _m
duplicates drop
save "/home/zqian/Documents/patents/data/cityname/MA1perc_cityname_match2", replace

use "/home/zqian/Documents/patents/data/cityname/MA1perc_cityname_match1", clear
append using "/home/zqian/Documents/patents/data/cityname/MA1perc_cityname_match2"
save "/home/zqian/Documents/patents/data/cityname/MA1perc_cityname_cleaned", replace




