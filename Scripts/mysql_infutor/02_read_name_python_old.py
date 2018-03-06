# Rose Tan
# Franklin Qian
# Date created: 2/12/2018
# Last edited: 2/22/2018
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
from itertools import chain

#################### SET PARAMETERS #######################

# set directory
workdir = "/ifs/gsb/tmcquade/BDMProject/SiliconValleyLabor/Data/"
os.chdir(workdir)

# input directory
inputdir = "raw_chunked/"

# outfile name or path
outputdir = 'name_csv/'

# chunk txt files
num_rows = 500000

# number of cores to use for multi-processing
num_cores = 32

######################################@#################### 

def processInput(filename, inputdir, outputdir, num_rows):
	if filename.endswith(".txt"):

		print("Processing Name file: " + filename)

		infile = inputdir + filename

		for word in keywords: 

			mydir = outputdir + word + "/"
			outfile = mydir + filename.replace(".txt","").replace("CRD3_","") + '_' + word + '_name'

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
				col1 = [i for i in range(0, 12)]
				col2 = [17,18]
				col3 = list(chain(range(22,86,7), 
								  range(23,87,7), 
								  range(24,88,7), 
								  range(25,89,7), 
								  range(26,90,7), 
								  range(27,91,7), 
								  range(28,92,7)))
				cols = col1 + col2 + col3
				df = pd.read_csv(infile, 
								 delimiter = '\t', # tab delimiter
								 header=None, # no header in data
								 #dtype = column_types, # take specific types
								 quoting=csv.QUOTE_NONE,
								 skiprows=rowlist,
								 usecols = cols)     

				#print('Memory usage (in bytes): ' + str(df.memory_usage(index=True,deep=True).sum()))

				# rename columns                        
				df.rename(columns={df.columns[0]: "pid",
								   df.columns[1]: "id_primary",
								   df.columns[2]: "id_primary_seq",
								   df.columns[3]: "id_second",
								   df.columns[4]: "name_prefix",
								   df.columns[5]: "name_first",
								   df.columns[6]: "name_middle",
								   df.columns[7]: "name_last",
								   df.columns[8]: "name_suffix",
								   df.columns[9]: "gender",
								   df.columns[10]: "dob",
								   df.columns[11]: "deceased",
								   df.columns[12]: "date_orig",
								   df.columns[13]: "date_last"
								  }, 
						  inplace = True)

				# rename columns
				for i in range(1,11):
					col = 14+(i-1)*7
					df.rename(columns={df.columns[col]: 'alias'+str(i),
									   df.columns[col+1]: 'alias_prefix'+str(i),
									   df.columns[col+2]: 'alias_first'+str(i),
									   df.columns[col+3]: 'alias_middle'+str(i),
									   df.columns[col+4]: 'alias_last'+str(i),
									   df.columns[col+5]: 'alias_suffix'+str(i),
									   df.columns[col+6]: 'alias_gender'+str(i),
									  }, 
							  inplace = True)

				# reshape wide to long
				df = pd.wide_to_long(df, ["alias", "alias_prefix", "alias_first",
										  "alias_middle", "alias_last", "alias_suffix", 
										  "alias_gender"], i="pid", j="alias_num")

				# drop duplicates
				df.reset_index(inplace=True) # duplicates doesn't count indices
				df = df.drop_duplicates(df.columns.difference(['alias_num']))

				#print('Memory usage after reshape (in bytes): ' 
				#	+ str(df.memory_usage(index=True,deep=True).sum()))
					
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
			os.remove(word + "_name.csv") # remove main file
		except OSError:
			pass
		interesting_files = glob.glob("*.csv") 
		df_list = pd.concat((pd.read_csv(f, header = 0) for f in interesting_files))
		df_list.to_csv(word + "_name.csv", index = False)
		for f in interesting_files:
			os.remove(f)

# main
if __name__ == "__main__":

	# timer
	start = time.time()

	keywords = [''.join(i) for i in product(ascii_uppercase, repeat = 2)]
	#keywords = ['AA','AB']

	with Parallel(n_jobs=num_cores) as parallel:
		parallel(delayed(processInput)(filename, inputdir, outputdir, num_rows) for filename in os.listdir(inputdir))
		parallel(delayed(writeOutput)(word, workdir, outputdir) for word in keywords)

	# time
	end = time.time()
	print("Time elapsed (in seconds): " + str(end - start))
