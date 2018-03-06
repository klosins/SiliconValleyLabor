# Rose Tan
# Franklin Qian
# Date created: 2/15/2018
# Last edited: 2/21/2018
#
# Description: 	This file takes in the raw infutor text file, 
# 				cleans the data, and
# 				converts it into a csv 
#
#
# TO DO: 
#
#
# 		

import os
import getpass # to detect user
import pandas as pd
import time
import csv
from itertools import product
from string import ascii_uppercase
import string
import glob
from joblib import Parallel, delayed
import multiprocessing

#################### SET PARAMETERS #######################

# set directory
if getpass.getuser() == 'rose':
	workdir = "/Users/rose/Dropbox (Diamond)/migration/data/"
	inputdir = "infutor_1perc/" # input directory
	num_cores = 6 # set number of cores
elif getpass.getuser() == 'zqian':
	workdir = "/media/zqian/Seagate Backup Plus Drive/infutor_1perc/data/"
	inputdir = "raw_chunked/" # input directory
	num_cores = 28 # set number of cores
elif getpass.getuser() == 'zqian1':
	workdir = "/ifs/gsb/tmcquade/BDMProject/SiliconValleyLabor/Data/infutor_1perc/data/"
	inputdir = "raw_chunked/" # input directoryls 
	num_cores = 32 # set number of cores


# change directory
os.chdir(workdir)

# outfile name or path
outputdir = 'address_csv/'

# chunk txt files
num_rows = 500000

# number of cores to use for multi-processing
#num_cores = multiprocessing.cpu_count() #max number on your workstation


########################################################### 

def processInput(filename, inputdir, outputdir, num_rows):
	if filename.endswith(".txt"):

		print("Processing Name file: " + filename)

		infile = inputdir + filename

		for word in keywords: 

			mydir = outputdir + word + "/"
			outfile = mydir + filename.replace(".txt","").replace("CRD3_","") + '_' + word + '_address'

			reader = pd.read_csv(infile, 
			                 delimiter = '\t', # tab delimiter
			                 header=None, # no header in data
			                 #dtype = column_types, # take specific types
			                 quoting=csv.QUOTE_NONE,
			                 chunksize = num_rows, # chunk txt file
			                 usecols = [7])

			# indices for rows to read in
			rowlist = []

			for df in reader:
			    df = df.loc[~df[7].str.startswith(word, na=False)]
			    rowlist += list(df.index.values)

			# read in csv, only lines with useful content
			try: 
				df = pd.read_csv(infile, 
				                 delimiter = '\t', # tab delimiter
				                 header=None, # no header in data
				                 #dtype = column_types, # take specific types
				                 quoting=csv.QUOTE_NONE,
				                 skiprows=rowlist,
				                 usecols = [0,5,6,7,
				                            *range(93,319,25),
				                            *range(95,321,25),
				                            *range(96,322,25),
				                            *range(97,323,25),
				                            *range(98,324,25),
				                            *range(99,325,25),
				                            *range(101,327,25),
				                            *range(102,328,25),
				                            *range(103,329,25),
				                            *range(104,330,25),
				                            *range(109,335,25),
				                            *range(112,338,25),
				                            *range(113,339,25),
				                            *range(114,340,25),
				                            *range(116,342,25)])     

				#print('Memory usage (in bytes): ' + str(df.memory_usage(index=True,deep=True).sum()))


				# rename columns                        
				df.rename(columns={df.columns[0]: "pid",
									df.columns[1]: "name_first",
					                df.columns[2]: "name_middle",
				                    df.columns[3]: "name_last"}, 
				          inplace = True)

				# rename columns
				for i in range(1,11):
				    col = 4+(i-1)*15
				    df.rename(columns={df.columns[col]: 'add'+str(i),
				                       df.columns[col+1]: 'add_stnum'+str(i),
				                       df.columns[col+2]: 'add_stpre'+str(i),
				                       df.columns[col+3]: 'add_stname'+str(i),
				                       df.columns[col+4]: 'add_sttype'+str(i),
				                       df.columns[col+5]: 'add_stsuf'+str(i),
				                       df.columns[col+6]: 'add_aptnum'+str(i),
				                       df.columns[col+7]: 'add_city'+str(i),
				                       df.columns[col+8]: 'add_state'+str(i),
				                       df.columns[col+9]: 'add_zip'+str(i),
				                       df.columns[col+10]: 'add_fips'+str(i),
				                       df.columns[col+11]: 'date_eff'+str(i),
				                       df.columns[col+12]: 'date_beg'+str(i),
				                       df.columns[col+13]: 'add_id'+str(i),
				                       df.columns[col+14]: 'date_end'+str(i),
				                      }, 
				              inplace = True)

				# reshape wide to long
				df = pd.wide_to_long(df, ["add_id", "date_eff", "date_beg", "date_end",
				                          "add", "add_stnum", "add_stpre", "add_stname", 
				                          "add_sttype", "add_stsuf", "add_aptnum", "add_city",
				                          "add_state", "add_zip", "add_fips"], i=["pid", 
				                          "name_first", "name_middle", "name_last"], j="addnum")  

				#print('Memory usage after reshape (in bytes): ' 
				#	+ str(df.memory_usage(index=True,deep=True).sum()))

				# drop if missing address id
				df = df[df['add_id'].notnull()]
				    
				# make csv
				if not os.path.exists(mydir):
					os.makedirs(mydir)
				outpath = outfile + '.csv'
				df.to_csv(outpath, index=True)
				#print(outpath + ' file completed')

			except:
				continue

def writeOutput(word, workdir, outputdir):
	mydir = workdir + outputdir + word + "/"
	if os.path.exists(mydir):
		os.chdir(mydir)
		try:
			os.remove(word + "_address.csv") # remove main file
		except OSError:
			pass
		interesting_files = glob.glob("*.csv") 
		df_list = pd.concat((pd.read_csv(f, header = 0) for f in interesting_files))
		df_list.to_csv(word + "_address.csv", index = False)
		for f in interesting_files:
			os.remove(f)

# main
if __name__ == "__main__":

	# timer
	start = time.time()

	keywords = [''.join(i) for i in product(ascii_uppercase, repeat = 2)]
	#keywords = ['AA','AB','AC','AD']

	with Parallel(n_jobs=num_cores) as parallel:
		parallel(delayed(processInput)(filename, inputdir, outputdir, num_rows) for filename in os.listdir(inputdir))
		parallel(delayed(writeOutput)(word, workdir, outputdir) for word in keywords)

	# time
	end = time.time()
	print("Time elapsed (in seconds): " + str(end - start))
