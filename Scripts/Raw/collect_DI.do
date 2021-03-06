version 15.0
clear all
cap log close
set more off

*#####################################################################
*Collect DI file from each state
*#####################################################################

local inpath "/media/zqian/Seagate Backup Plus Drive/infutor_lastname"
local outpath "/media/zqian/Seagate Backup Plus Drive/infutor_DI"
//store all file names in folder
local subfolders: dir "`inpath'" dirs "*"

foreach state in `subfolders' { 
	noisily: dis "processing `state'"
	
	//check if output directory for each state exists
	cap confirmdir "`outpath'/`state'"
	if `r(confirmdir)' != 0 {
		mkdir "`outpath'/`state'"
		cp "`inpath'/`state'/DI.csv" "`outpath'/`state'/DI.csv"	
	} 
}


clear
foreach state in `subfolders' { 

 preserve
 insheet using "`outpath'/`state'/DI.csv", clear
 save temp, replace
 restore
 append using temp
}
erase temp.dta
tostring dob, replace
export delimited using "/home/zqian/Dropbox/SiliconValleyLabor/Data/DI_Infutor/full_DI_2.csv", replace
