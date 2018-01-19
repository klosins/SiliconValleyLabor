version 15.0
clear all
cap log close
set more off

import delimited /home/zqian/Documents/patents/data/inventor_raw.csv, clear
sort lastname patent

//first 2 letters of last name
gen last2=substr(lastname,1,2)
lab var last2 "first 2 letters of last name"

//export inventor data
save /home/zqian/Documents/patents/data/inventor, replace
export delimited /home/zqian/Documents/patents/data/inventor.csv, replace
savesome if last2=="DI" using /home/zqian/Documents/patents/data/inventor_DI, replace

//load infutor names (DI from CT)
import delimited "/media/zqian/Seagate Backup Plus Drive/infutor_lastname/ct/DI.csv", clear

//merge with inventor names (DI)
rename name_last lastname
rename name_first firstname_infutor
//just keep last address
*keep if addnum==1
joinby lastname using /home/zqian/Documents/patents/data/inventor_DI, unmatched(both)
br if firstname_infutor==firstname & add_state==state
