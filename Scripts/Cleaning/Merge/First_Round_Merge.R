#========================
# First Round Merge 
#========================
# Started by Sylvia Klosin (sylviaklosin@gmail.com) on Jan 17th 2018  
# In this document we will start 

#========================
# Section 0: Setup
#========================

#=========
# 0.1 Import Packages
#=========
library(data.table)
library(dplyr)
library(here)

#=========
# 0.2 Set working directory 
#=========
setwd(here::here("RawData"))

#=========
# Importing data
#=========
patent <- fread("inventor.csv")
infutor <- fread("DI.csv")

# since this is practice case 

patent <- patent[last2 == "DI",]

#========================
# Section 1: Making consistent column names 
#========================

patent %>% rename(add_city = city,
                  add_state = state, 
                  zip = zipcode, 
                  name_first = firstname,
                  name_last = lastname) -> patent
setDT(patent)


#========================
# Section 2: Cleaning patent data  
#========================

# remove people that do not live in the US, we will not have a match for them 

patent <- patent[country == "US",]

#========================
# Section 3: Removing duplicate information
#========================

# Rows that are identical in the data are not needed in the patent data. 
# Like if a person has multiple patents, but live in the same place for them, 
# we dont need all that data for the merging exersize. 

unique(patent[,.(name_first, name_last, add_city, add_state)]) -> patent

# in the DI case this removes 
#========================
# Section 4: Merging
#========================

patent_infutor_merge <- merge(patent, infutor, 
                              all.x=TRUE, by = c("name_last", "add_state"),
                              allow.cartesian=TRUE) # doing a LEFT OUTER JOIN
# returns all the rows from the left table, filling in matched columns (or NA) from the right table
# if there are multiple rows from the right table match to a row in the left table, then new rows 
# will be added to the left. 







