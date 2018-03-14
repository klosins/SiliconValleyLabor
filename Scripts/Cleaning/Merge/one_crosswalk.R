
#========================
# Section 0: Setup
#========================

# this code makes a crosswalk between the patent data and a two letter set of infutor data

#=========
# 0.1 Import Packages
#=========
library(data.table)
library(dplyr)
library(here)
library(ggplot2)
library(lubridate)
library(stringr)
library(xtable)
library(tidyverse)


#=========
# 0. 2 Importing data
#=========

start_time <-proc.time()

patent <- fread("/Volumes/Seagate Backup Plus Drive/patent/data/AA/patent_AA.csv")
last_names <- unique(patent[,(name_last_clean)])

infutor_address <- fread("/Volumes/Seagate Backup Plus Drive/infutor/data/address_csv/AA/AA_address.csv")
infutor_name <- fread("/Volumes/Seagate Backup Plus Drive/infutor/data/name_csv/AA/AA_name.csv", 
                      colClasses = list(character=18))
infutor_name <- infutor_name[alias_num == 1,]
infutor <- merge(infutor_address, infutor_name, by = "pid" )
#subsetting infutor to only our last names of interest                      
infutor <- infutor[name_last %in% last_names, ]



#=========
# 0. 3 Functions
#=========
removeWords <- function(str, stopwords) {
  x <- unlist(strsplit(str, " "))
  paste(x[!x %in% stopwords], collapse = " ")
}

#========================
# Section 1:  infutor data  
#========================

#=========
# 1.2 fix the date variables 
#=========
# for infutor data 
infutor[, addmonth_beg := as.Date(paste(addmonth_beg,"d1",sep=""), format = "%Ym%md%d")]
infutor[, addmonth_end := as.Date(paste(addmonth_end,"d1",sep=""), format = "%Ym%md%d")]
infutor[, first_seen := as.Date(paste(first_seen,"d1",sep=""), format = "%Ym%md%d")]
infutor[, last_seen := as.Date(paste(last_seen,"d1",sep=""), format = "%Ym%md%d")]


#=========
# 1.3 fixing the perfered city name
#=========

#infutor[pref_city_name == "", pref_city_name := add_city]
infutor[, pref_city_name := add_city]

#========================
# Section 2: Making new variables  
#========================

# cleaning last name 
bad_name_words <- c("JR", "SR", "II", "III", "IV") # words we dont want in last names

infutor[, first1 := substr(name_first, start = 1, stop = 1)] # first letters of first name
infutor[, first2 := substr(name_first, start = 1, stop = 2)] # first two letters of first name
infutor[, first3 := substr(name_first, start = 1, stop = 3)] # first three letters of first name
infutor[, name_first_clean := name_first] 
infutor[, name_last_clean := name_last]
infutor[, name_middle_clean := name_middle]
infutor[, middle1 := substr(name_middle_clean, start = 1, stop = 1)] # first letter middle


#=======
# 2.2: creating new PID vars 
#=======
infutor_base <- infutor[,.(pid, add_id, name_first, name_last, dob)]
infutor_base <- unique(infutor_base[!is.na(dob),])
infutor_base_new <- infutor_base[,pid_new := paste0(pid, collapse = "-"),
                                 .(add_id, name_first, name_last, dob)]
infutor_base_new_ids <- unique(infutor_base_new[,.(pid, pid_new)]) 
# some pid are matched to many pid_new values, where some of the pid_new values are subsets, 
# so we fix this below # do this due to people move, see pid == 135609
infutor_base_new_ids <- unique(infutor_base_new_ids[,pid_new := max(pid_new), .(pid)])
infutor<- merge(infutor, infutor_base_new_ids, by = "pid", all.x = TRUE)
infutor <- infutor[is.na(pid_new), pid_new := as.character(pid)]


#========================
# Section 3: Removing duplicate information
#========================
# Rows that are identical in the data are not needed in the patent data. 
# Like if a person has multiple patents, but live in the same place for them, 
# we dont need all that data for the merging exersize. 

unique(patent[,.(name_first_clean,
                 first3,
                 name_last,
                 name_last_clean, 
                 pref_city_name,
                 add_state,
                 unique_inventor_id_new)]) -> unique_patent


#========================
# Section 4: Merging
#========================

# We merge on last name, state, city and the first three letters of the first name. 

patent_infutor_merge <- merge(unique_patent, infutor, 
                              all.x=TRUE, by = c("name_last_clean", "pref_city_name", 
                                                 "add_state","first3"),
                              allow.cartesian=TRUE) # doing a LEFT OUTER JOIN
# returns all the rows from the left table, filling in matched columns (or NA) from the right table
# if there are multiple rows from the right table match to a row in the left table, then new rows 
# will be added to the left. 


#========================
# Section 5: Merge stats
#========================

patent_infutor_merge_pairs <- unique(patent_infutor_merge[,.(unique_inventor_id_new, pid_new)])



### 1-1 matches 
#=========
# How many matches does one inventor get 
#=========
patent_infutor_merge_pairs_no_na <- patent_infutor_merge_pairs[!is.na(pid_new), ]
patent_infutor_merge_pairs_inventor <- patent_infutor_merge_pairs_no_na[,.N,.(unique_inventor_id_new)]
no_match <- patent[(!(unique_inventor_id_new %in% patent_infutor_merge_pairs_inventor$unique_inventor_id_new)),]


patent_one_match <- patent_infutor_merge_pairs_inventor[N == 1,]
patent_one_match <- merge(patent_one_match,
                          patent_infutor_merge,
                          by = "unique_inventor_id_new")
patent_one_match <- patent_one_match[!is.na(pid_new),] # start of our crosswalk
patent_one_match[, same_first := (name_first_clean.x == name_first_clean.y)]




### > 1 matches 

#=========
# People who get some matches, but not unique in first merge 
#=========
patent_many_match <- patent_infutor_merge_pairs_inventor[N >= 2,]
patent_many_match <- merge(patent_many_match,
                           patent_infutor_merge,
                           by = "unique_inventor_id_new")

#=========
# seeing if we can bring in unique match by using full first name 
#=========

patent_many_match[, same_first := (name_first_clean.x == name_first_clean.y)]

patent_many_match_first_name_pairs <- patent_many_match[same_first == TRUE,
                                                        .(sum_pid_new =length(unique(pid_new))),.(unique_inventor_id_new)]

# bringing in the new matches (determined by full first name) into "one match"
patent_one_match <- rbind(patent_one_match, patent_many_match[(unique_inventor_id_new %in%
                                                                 patent_many_match_first_name_pairs[sum_pid_new == 1,
                                                                                                    (unique_inventor_id_new)] & (same_first == TRUE)),])
# removing the observations for people that have one right match from patent_many_match
patent_many_match <- patent_many_match[!(unique_inventor_id_new %in%
                                           patent_many_match_first_name_pairs[sum_pid_new == 1,
                                                                              (unique_inventor_id_new)]), ] 


#=========
# Next time we bring in first letter of middle name 
#=========

patent_did_merge <- merge(patent[unique_inventor_id_new %in% patent_many_match$unique_inventor_id_new,], 
                          patent_many_match, by = "unique_inventor_id_new", allow.cartesian = T )

patent_did_merge[, same_middle := (middle1.x == middle1.y)]

patent_did_merge_sum <- patent_did_merge[, .(sum_match = sum(same_middle)), .(unique_inventor_id_new, pid_new)]

at_least_one_good_middle <- patent_did_merge_sum[sum_match > 0,]

one_good_middle <- at_least_one_good_middle[,.N,.(unique_inventor_id_new)][N == 1, .(unique_inventor_id_new)]

#=========
# people with only one good date match
#=========

one_good_date_pair <- at_least_one_good_middle[unique_inventor_id_new %in% one_good_middle$unique_inventor_id_new,
                                               .(unique_inventor_id_new, pid_new) ]

patent_many_match_good_middle <- merge(one_good_date_pair, patent_many_match, by = c("unique_inventor_id_new", "pid_new"))


# bringing in the new matches into "one match"
patent_one_match <- rbind(patent_one_match, patent_many_match_good_middle)
# removing the observations for people that have one right match
patent_many_match <- patent_many_match[!(unique_inventor_id_new %in% one_good_date_pair$unique_inventor_id_new), ] 



#### Now we check bad dates

patent_did_merge[, within_date := ((addmonth_beg - 180) < appdate_clean  & appdate_clean < (addmonth_end + 180))]

patent_did_merge_sum <- patent_did_merge[, .(sum_match = sum(within_date)), .(unique_inventor_id_new, pid_new)]
at_least_one_good_date <-  patent_did_merge_sum[sum_match > 0,]

one_good_date <- at_least_one_good_date[,.N,.(unique_inventor_id_new)][N == 1, .(unique_inventor_id_new)]

# people with only one good date match

one_good_date_pair <- at_least_one_good_date[unique_inventor_id_new %in% one_good_date$unique_inventor_id_new,
                                             .(unique_inventor_id_new, pid_new) ]

patent_many_match_good_date <- merge(one_good_date_pair, patent_many_match, by = c("unique_inventor_id_new", "pid_new"))

# bringing in the new matches into "one match"
patent_one_match <- rbind(patent_one_match, patent_many_match_good_date)
# removing the observations for people that have one right match
patent_many_match <- patent_many_match[!(unique_inventor_id_new %in% 
                                           one_good_date_pair$unique_inventor_id_new), ] 


#========================
# Section 6: Write Crosswalk
#========================

crosswalk <- unique(patent_one_match[,.(unique_inventor_id_new, pid_new)])

end_time <- proc.time()


fwrite(crosswalk, "crosswalk.csv")


