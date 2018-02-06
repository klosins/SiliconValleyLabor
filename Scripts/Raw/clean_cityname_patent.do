version 15.0
clear all
cap log close
set more off
cd "~/Dropbox/SiliconValleyLabor/Data/CityName/"

import delimited "/home/zqian/Documents/patents/data/invpat_full_disambiguation.csv", clear
rename (city state zipcode) (add_city add_state add_zip)
keep add_city add_state add_zip
sort add_city add_zip

//standardize cardinal directions
replace add_city = trim(add_city)
replace add_city = " " + add_city + " "
replace add_city = subinstr(add_city, " E ", " EAST ", .) if regex(add_city, " E ")
replace add_city = subinstr(add_city, " W ", " WEST ", .) if regex(add_city, " W ")
replace add_city = subinstr(add_city, " S ", " SOUTH ", .) if regex(add_city, " S ")
replace add_city = subinstr(add_city, " N ", " NORTH ", .) if regex(add_city, " N ")
replace add_city = trim(add_city)

tostring add_zip, format(%05.0f) replace
*replace add_zip="" if add_zip=="."
drop if add_city==""
duplicates drop

unique add_city add_state
/*
Number of unique values of add_city add_state is  24852
Number of records is  25074
*/

//first exact merge based on city name and zip to get preferred name
mmerge add_city add_zip using "cityname_std", ///
	type(n:1) unmatched(both) umatch(city_name zip)
/*
         ------------+---------------------------------------------------------
              _merge |  71831  obs only in master data                (code==1)
                     |  43284  obs only in using data                 (code==2)
                     |  21204  obs both in master and using data      (code==3)
-------------------------------------------------------------------------------
*/

preserve
keep if _m==3
drop _m
keep add_city add_state add_zip pref_city_name
duplicates drop

//(city,state) with multiple matches 
gen count=1 
bys add_city add_state (pref_city_name): replace count=0 if pref_city_name==pref_city_name[_n-1]
bys add_city add_state (pref_city_name): egen nmatch=total(count)
drop count
tab nmatch
/*
     nmatch |      Freq.     Percent        Cum.
------------+-----------------------------------
          1 |     21,142       99.71       99.71
          2 |         62        0.29      100.00
------------+-----------------------------------
      Total |     21,204      100.00
*/

unique add_city add_state
/*
Number of unique values of add_city add_state is  21100
Number of records is  21204
*/

save "~/Dropbox/SiliconValleyLabor/Temp/CityName/invpat_cityname_match1", replace
restore

keep if _m==1
keep add_city add_state add_zip
save "~/Dropbox/SiliconValleyLabor/Temp/CityName/invpat_cityname_unmatch1", replace

use "~/Dropbox/SiliconValleyLabor/Temp/CityName/invpat_cityname_unmatch1", clear
drop add_zip
duplicates drop
unique add_city add_state
/*
Number of unique values of add_city add_state is  3854
Number of records is  3854
*/

//second merge based on (city,state)
drop if mi(add_state)
mmerge add_city add_state using "cityname_std", ///
	type(1:n) unmatched(both) umatch(city_name state_abbr)

/*
         ------------+---------------------------------------------------------
              _merge |   2522  obs only in master data                (code==1)
                     |  60114  obs only in using data                 (code==2)
                     |   4374  obs both in master and using data      (code==3)
-------------------------------------------------------------------------------
*/	

keep if _m==3
drop _m
keep add_city add_state pref_city_name
duplicates drop

//(city,state) with multiple matches 
gen count=1 
bys add_city add_state (pref_city_name): replace count=0 if pref_city_name==pref_city_name[_n-1]
bys add_city add_state (pref_city_name): egen nmatch=total(count)
drop count
tab nmatch 
/*
     nmatch |      Freq.     Percent        Cum.
------------+-----------------------------------
          1 |      1,130       67.46       67.46
          2 |        286       17.07       84.54
          3 |         99        5.91       90.45
          4 |         52        3.10       93.55
          5 |         25        1.49       95.04
          6 |         18        1.07       96.12
          8 |          8        0.48       96.60
         10 |         10        0.60       97.19
         13 |         13        0.78       97.97
         17 |         34        2.03      100.00
------------+-----------------------------------
      Total |      1,675      100.00
*/

unique add_city add_state
/*
Number of unique values of add_city add_state is  1332
Number of records is  1675
*/

mmerge add_city add_state using "~/Dropbox/SiliconValleyLabor/Temp/CityName/invpat_cityname_unmatch1", ///
	type(n:n) unmatched(using)
	
preserve
keep if _m==3
unique add_city add_state
/*
Number of unique values of add_city add_state is  1332
Number of records is  1697
*/
drop _m
duplicates drop
save "~/Dropbox/SiliconValleyLabor/Temp/CityName/invpat_cityname_match2", replace
restore

keep if _m==2
//number of (city,state) not matched
unique add_city add_state
/*
Number of unique values of add_city add_state is  2522
Number of records is  2523
*/
drop _m
duplicates drop
save "~/Dropbox/SiliconValleyLabor/Temp/CityName/invpat_cityname_unmatch2", replace

use "~/Dropbox/SiliconValleyLabor/Temp/CityName/invpat_cityname_match1", clear
append using "~/Dropbox/SiliconValleyLabor/Temp/CityName/invpat_cityname_match2"
append using "~/Dropbox/SiliconValleyLabor/Temp/CityName/invpat_cityname_unmatch2"
save "~/Dropbox/SiliconValleyLabor/Data/CityName/invpat_cityname_cleaned", replace
export delimited using "/home/zqian/Dropbox/SiliconValleyLabor/Data/CityName/invpat_cityname_cleaned.csv", replace

unique add_city add_state add_zip
/*
Number of unique values of add_city add_state add_zip is  25074
Number of records is  25424
*/

unique add_city add_state add_zip if mi(pref_city_name)
/*
Number of unique values of add_city add_state add_zip is  2523
Number of records is  2523
*/

drop nmatch
gen count=1 
bys add_city add_state add_zip (pref_city_name): replace count=0 if pref_city_name==pref_city_name[_n-1]
bys add_city add_state add_zip (pref_city_name): egen nmatch=total(count)
drop count
tab nmatch

/*
     nmatch |      Freq.     Percent        Cum.
------------+-----------------------------------
          0 |      2,523        9.92        9.92
          1 |     22,346       87.89       97.82
          2 |        288        1.13       98.95
          3 |         99        0.39       99.34
          4 |         60        0.24       99.58
          5 |         25        0.10       99.67
          6 |         18        0.07       99.74
          8 |          8        0.03       99.78
         10 |         10        0.04       99.82
         13 |         13        0.05       99.87
         17 |         34        0.13      100.00
------------+-----------------------------------
      Total |     25,424      100.00
*/
