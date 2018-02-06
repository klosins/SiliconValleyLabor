version 15.0
clear all
cap log close
set more off
cd "~/Dropbox/SiliconValleyLabor/Data/CityName/"

********************************************************************************
* USPS list of city names and their preferred names by zipcode
********************************************************************************
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

save "cityname_std.dta", replace
export delimited using "cityname_std.csv", replace

********************************************************************************
* Standardize city names in infutor MA 1% file
********************************************************************************
use "/media/zqian/Seagate Backup Plus Drive/infutor_1perc/data/dta/address/MA1perc_address", clear
keep add_city add_state add_zip add_fips
sort add_city add_zip

//standardize cardinal directions
replace add_city = trim(add_city)
replace add_city = " " + add_city + " "
replace add_city = subinstr(add_city, " E ", " EAST ", .) if regex(add_city, " E ")
replace add_city = subinstr(add_city, " W ", " WEST ", .) if regex(add_city, " W ")
replace add_city = subinstr(add_city, " S ", " SOUTH ", .) if regex(add_city, " S ")
replace add_city = subinstr(add_city, " N ", " NORTH ", .) if regex(add_city, " N ")
replace add_city = trim(add_city)

tostring add_fips, format(%03.0f) replace
tostring add_zip, format(%05.0f) replace
drop if add_city==""
duplicates drop

unique add_city add_state add_fips
/*
Number of unique values of add_city add_state add_fips is  4623
Number of records is  7414
*/

//first exact merge based on city name and zip to get preferred name
mmerge add_city add_zip using "cityname_std", ///
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
keep add_city add_state add_zip add_fips pref_city_name
duplicates drop

//(city,state,county) with multiple matches 
gen count=1 
bys add_city add_state add_fips (pref_city_name): replace count=0 if pref_city_name==pref_city_name[_n-1]
bys add_city add_state add_fips (pref_city_name): egen nmatch=total(count)
drop count
tab nmatch 
/*
     nmatch |      Freq.     Percent        Cum.
------------+-----------------------------------
          1 |      6,588       95.26       95.26
          2 |        240        3.47       98.73
          3 |         36        0.52       99.25
          4 |          4        0.06       99.31
          6 |          6        0.09       99.39
          7 |         10        0.14       99.54
         13 |         32        0.46      100.00
------------+-----------------------------------
      Total |      6,916      100.00
*/

unique add_city add_state add_fips
/*
Number of unique values of add_city add_state add_fips is  4376
Number of records is  6916
*/

save "~/Dropbox/SiliconValleyLabor/Temp/CityName/MA1perc_cityname_match1", replace
restore

keep if _m==1
keep add_city add_state add_fips add_zip
save "~/Dropbox/SiliconValleyLabor/Temp/CityName/MA1perc_cityname_unmatch1", replace

use "~/Dropbox/SiliconValleyLabor/Temp/CityName/MA1perc_cityname_unmatch1", clear
drop add_zip
duplicates drop
unique add_city add_state add_fips
/*
Number of unique values of add_city add_state add_fips is  418
Number of records is  418
*/

//second merge based on (city,state,fips)
mmerge add_city add_state add_fips using "cityname_std", ///
	type(1:n) unmatched(both) umatch(city_name state_abbr county_fips)

/*
------------+---------------------------------------------------------
              _merge |    231  obs only in master data                (code==1)
                     |  62527  obs only in using data                 (code==2)
                     |   1961  obs both in master and using data      (code==3)
-------------------------------------------------------------------------------
*/	

keep if _m==3
drop _m
keep add_city add_state add_fips pref_city_name
duplicates drop

//(city,state,county) with multiple matches 
gen count=1 
bys add_city add_state add_fips (pref_city_name): replace count=0 if pref_city_name==pref_city_name[_n-1]
bys add_city add_state add_fips (pref_city_name): egen nmatch=total(count)
drop count
tab nmatch 
/*
     nmatch |      Freq.     Percent        Cum.
------------+-----------------------------------
          1 |        147       55.47       55.47
          2 |         50       18.87       74.34
          3 |         18        6.79       81.13
          4 |         16        6.04       87.17
          5 |         15        5.66       92.83
          6 |          6        2.26       95.09
         13 |         13        4.91      100.00
------------+-----------------------------------
      Total |        265      100.00
*/

unique add_city add_state add_fips
/*
Number of unique values of add_city add_state add_fips is  187
Number of records is  265
*/

mmerge add_city add_state add_fips using "~/Dropbox/SiliconValleyLabor/Temp/CityName/MA1perc_cityname_unmatch1", ///
	type(n:n) unmatched(using)
	
preserve
keep if _m==3
unique add_city add_state add_fips
/*
Number of unique values of add_city add_state add_fips is  187
Number of records is  448
*/
drop _m
duplicates drop
save "~/Dropbox/SiliconValleyLabor/Temp/CityName/MA1perc_cityname_match2", replace
restore

keep if _m==2
//number of (city,state,county) not matched
unique add_city add_state add_fips
/*
Number of unique values of add_city add_state add_fips is  231
Number of records is  269
*/
drop _m
duplicates drop
save "~/Dropbox/SiliconValleyLabor/Temp/CityName/MA1perc_cityname_unmatch2", replace

use "~/Dropbox/SiliconValleyLabor/Temp/CityName/MA1perc_cityname_unmatch2", clear
keep add_city add_state
duplicates drop

//third merge based on (city,state)
mmerge add_city add_state using "cityname_std", ///
	type(1:n) unmatched(both) umatch(city_name state_abbr)
/*	
------------+---------------------------------------------------------
              _merge |    200  obs only in master data                (code==1)
                     |  64136  obs only in using data                 (code==2)
                     |    352  obs both in master and using data      (code==3)
-------------------------------------------------------------------------------
*/

use "~/Dropbox/SiliconValleyLabor/Temp/CityName/MA1perc_cityname_match1", clear
append using "~/Dropbox/SiliconValleyLabor/Temp/CityName/MA1perc_cityname_match2"
append using "~/Dropbox/SiliconValleyLabor/Temp/CityName/MA1perc_cityname_unmatch2"
save "~/Dropbox/SiliconValleyLabor/Data/CityName/MA1perc_cityname_cleaned", replace





