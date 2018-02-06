version 15.0
clear all
cap log close
set more off

//standardized city names from USPS
use "~/Dropbox/SiliconValleyLabor/Data/CityName/city_county_unique.dta", clear
unique county_fips city_key
unique unique_city_key

rename city city_name
merge 1:m county_fips city_key using "~/Dropbox/SiliconValleyLabor/Data/CityName/ctystate_detail", assert(3) keep(3) nogen

unique city_key zip
unique city_name zip

save "~/Dropbox/SiliconValleyLabor/Data/CityName/cityname_std", replace
export delimited using "~/Dropbox/SiliconValleyLabor/Data/CityName/cityname_std.csv", replace

//clean city names in patent data
import delimited "/Users/qzjquantum/Downloads/all_bad_matches.csv", encoding(ISO-8859-1) clear
tostring zip, format(%05.0f) replace
rename zip add_zip
keep add_city add_state add_zip
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

unique add_city add_state add_zip 
/*
Number of unique values of add_city add_state add_zip is  1098
Number of records is  1098
*/

