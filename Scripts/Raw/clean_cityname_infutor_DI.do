version 15.0
clear all
cap log close
set more off
cd "~/Dropbox/SiliconValleyLabor/Data/CityName/"

import delimited "/home/zqian/Dropbox/SiliconValleyLabor/Data/DI_Infutor/full_DI_2.csv", clear 
*keep add_city add_state add_zip add_fips
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

*tostring add_fips, format(%03.0f) replace
*replace add_fips="" if add_fips=="."
tostring add_zip, format(%05.0f) replace
*replace add_zip="" if add_zip=="."
drop if add_city==""
duplicates drop

*unique add_city add_state add_fips
unique add_city add_state
/*
Number of unique values of add_city add_state is  31869
Number of records is  44848
*/

//first exact merge based on city name and zip to get preferred name
mmerge add_city add_zip using "cityname_std", ///
	type(n:1) unmatched(both) umatch(city_name zip)
/*
         ------------+---------------------------------------------------------
              _merge |  10301  obs only in master data                (code==1)
                     |  29906  obs only in using data                 (code==2)
                     |  34582  obs both in master and using data      (code==3)
-------------------------------------------------------------------------------
*/

preserve
keep if _m==3
drop _m
*keep add_city add_state add_zip add_fips pref_city_name
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
          1 |     30,635       88.59       88.59
          2 |      2,267        6.56       95.14
          3 |        729        2.11       97.25
          4 |        350        1.01       98.26
          5 |        195        0.56       98.83
          6 |        156        0.45       99.28
         10 |         40        0.12       99.39
         13 |         31        0.09       99.48
         15 |         25        0.07       99.55
         16 |         46        0.13       99.69
         17 |         76        0.22       99.91
         26 |         32        0.09      100.00
------------+-----------------------------------
      Total |     34,582      100.00
*/

unique add_city add_state
/*
Number of unique values of add_city add_state is  26815
Number of records is  34582
*/

save "~/Dropbox/SiliconValleyLabor/Temp/CityName/infutor_DI_cityname_match1", replace
restore

keep if _m==1
keep add_city add_state add_zip
save "~/Dropbox/SiliconValleyLabor/Temp/CityName/infutor_DI_cityname_unmatch1", replace

use "~/Dropbox/SiliconValleyLabor/Temp/CityName/infutor_DI_cityname_unmatch1", clear
drop add_zip
duplicates drop
unique add_city add_state
/*
Number of unique values of add_city add_state is  7017
Number of records is  7017
*/

//second merge based on (city,state)
drop if mi(add_state)
mmerge add_city add_state using "cityname_std", ///
	type(1:n) unmatched(both) umatch(city_name state_abbr)

/*
         ------------+---------------------------------------------------------
              _merge |   4873  obs only in master data                (code==1)
                     |  54122  obs only in using data                 (code==2)
                     |  10366  obs both in master and using data      (code==3)
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
          1 |      1,748       60.86       60.86
          2 |        528       18.38       79.25
          3 |        216        7.52       86.77
          4 |        104        3.62       90.39
          5 |         75        2.61       93.00
          6 |         60        2.09       95.09
          7 |          7        0.24       95.33
          8 |          8        0.28       95.61
         10 |         10        0.35       95.96
         13 |         13        0.45       96.41
         15 |         15        0.52       96.94
         16 |         16        0.56       97.49
         17 |         34        1.18       98.68
         38 |         38        1.32      100.00
------------+-----------------------------------
      Total |      2,872      100.00
*/

unique add_city add_state
/*
Number of unique values of add_city add_state is  2144
Number of records is  2872
*/

mmerge add_city add_state using "~/Dropbox/SiliconValleyLabor/Temp/CityName/infutor_DI_cityname_unmatch1", ///
	type(n:n) unmatched(using)
	
preserve
keep if _m==3
unique add_city add_state
/*
Number of unique values of add_city add_state is  2144
Number of records is  7758
*/
drop _m
duplicates drop
save "~/Dropbox/SiliconValleyLabor/Temp/CityName/infutor_DI_cityname_match2", replace
restore

keep if _m==2
//number of (city,state) not matched
unique add_city add_state
/*
Number of unique values of add_city add_state is  4873
Number of records is  5518
*/
drop _m
duplicates drop
save "~/Dropbox/SiliconValleyLabor/Temp/CityName/infutor_DI_cityname_unmatch2", replace

use "~/Dropbox/SiliconValleyLabor/Temp/CityName/infutor_DI_cityname_match1", clear
append using "~/Dropbox/SiliconValleyLabor/Temp/CityName/infutor_DI_cityname_match2"
append using "~/Dropbox/SiliconValleyLabor/Temp/CityName/infutor_DI_cityname_unmatch2"
save "~/Dropbox/SiliconValleyLabor/Data/CityName/infutor_DI_cityname_cleaned", replace
export delimited using "/home/zqian/Dropbox/SiliconValleyLabor/Data/CityName/infutor_DI_cityname_cleaned.csv", replace

unique add_city add_state add_zip
/*
Number of unique values of add_city add_state add_zip is  44848
Number of records is  47858
*/

unique add_city add_state add_zip if mi(pref_city_name)
/*
Number of unique values of add_city add_state add_zip is  5518
Number of records is  5518
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
          0 |      5,518       11.53       11.53
          1 |     37,948       79.29       90.82
          2 |      1,552        3.24       94.07
          3 |        795        1.66       95.73
          4 |        560        1.17       96.90
          5 |        610        1.27       98.17
          6 |        126        0.26       98.43
          7 |         91        0.19       98.63
          8 |         32        0.07       98.69
         10 |         60        0.13       98.82
         13 |        182        0.38       99.20
         15 |         60        0.13       99.32
         16 |        112        0.23       99.56
         17 |        136        0.28       99.84
         38 |         76        0.16      100.00
------------+-----------------------------------
      Total |     47,858      100.00
*/
