version 15.0
clear all
cap log close
set more off

*#####################################################################
*Reorganize infutor data by first 2 letters of lastname by each state
*#####################################################################

local inpath "/media/zqian/Seagate Backup Plus Drive/infutor_cleaned"
local outpath "/media/zqian/Seagate Backup Plus Drive/infutor_lastname"
//store all file names in folder
local files: dir "`inpath'" files "*"  

foreach file in `files' { 
	//picks up state name
	local filesub: subinstr local file "_long.dta" ""
	local state: subinstr local filesub "crd3_" ""
	noisily: dis "processing `state'"
	
	//check if output directory for each state exists
	cap confirmdir "`outpath'/`state'"
	if `r(confirmdir)' != 0 {
		mkdir "`outpath'/`state'"
		use "`inpath'/`file'", clear
		sort name_last pid addnum
		
		//first 2 letters of last name
		gen last2=substr(name_last,1,2)
		lab var last2 "first 2 letters of last name"
		
		//breaks data into files by last2
		qui levelsof last2, local(levels)
		foreach l of local levels {
			preserve
			keep if last2=="`l'"
			export delimited using "`outpath'/`state'/`l'.csv", replace
			restore 
			
		} 
	}
}
