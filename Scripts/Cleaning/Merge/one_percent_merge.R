
library(data.table)
library(devtools)
library(doParallel)
library(doMPI)
library(doSNOW)
library(foreach)
library(ggplot2)
library(parallel)
library(Rmpi) # how to install found here http://www.stats.uwo.ca/faculty/yu/Rmpi/mac_os_x.htm
library(tidyverse)



cl <- makeCluster(2)
registerDoParallel(cl)

x <- expand.grid(rep(list(LETTERS), 2))
list_letter_pairs <- sort(do.call(paste0, x))

foreach(i=(list_letter_pairs), .packages='data.table') %dopar% 
  name_letter_directory <- paste0("/Volumes/Seagate Backup Plus Drive/infutor_1perc/data/name_csv/", as.character(i))
name_current_DT <- paste0(as.character(i),"_name.csv")
current_DT <- fread(paste0(name_letter_directory,"/",name_current_DT), colClasses = list(character=18))
current_DT <- current_DT[1:10,]
name_crosswalk_directory <- c("/Volumes/Seagate Backup Plus Drive/infutor_1perc/crosswalk")
name_current_crosswalk <-  paste0("iris",as.character(i),".csv")
fwrite(current_DT, paste0(name_crosswalk_directory,"/",name_current_crosswalk))
stopCluster(cl)





cl <- makeCluster(2)
registerDoParallel(cl)

x <- expand.grid(rep(list(LETTERS), 2))
list_letter_pairs <- sort(do.call(paste0, x))

foreach(i=(list_letter_pairs), .packages='data.table') %dopar% 
  name_letter_directory <- paste0("/Volumes/Seagate Backup Plus Drive/infutor_1perc/data/name_csv/", i )
print(name_letter_directory)
stopCluster(cl)


cl <- parallel::makeCluster(1, "PSOCK")
doParallel::registerDoParallel(cl)
list_letter_pairs <- c("AB", "AC", "AD")
foreach(i=(list_letter_pairs), .packages='data.table') %dopar%{
  name_letter_directory = paste0("~/Desktop")
  name_current_DT = paste0(as.character(i),".csv")
  current_DT = fread(paste0(name_letter_directory,"/",name_current_DT))
  current_DT = current_DT[1:10,]
  name_crosswalk_directory = c("~/Desktop")
  name_current_crosswalk =  paste0("iris",as.character(i),".csv")
  fwrite(current_DT, paste0(name_crosswalk_directory,"/",name_current_crosswalk))
  }
stopCluster(cl)







registerDoParallel(cl)
foreach(i = 1:3, .combine=c) %dopar% {
  i**2
}
stopCluster(cl)





