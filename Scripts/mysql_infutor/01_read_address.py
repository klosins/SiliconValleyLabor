# Rose Tan
# Franklin Qian
# Date created: 2/12/2018
# Last edited: 2/22/2018
#
# Description:  This file takes in the raw infutor text file, 
#               cleans the data, and
#               converts it into a csv 
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

#################### SET PARAMETERS #######################

# set directory
#workdir = "/media/zqian/Seagate Backup Plus Drive/CRD3/"
#workdir = "/media/zqian/Seagate Backup Plus Drive/infutor_1perc/data/"
workdir = "/ifs/gsb/tmcquade/BDMProject/SiliconValleyLabor/Data/"
os.chdir(workdir)

# input directory
inputdir = "raw_chunked/"

# outfile name or path
outputdir = 'address_csv/'

# chunk txt files
num_rows = 500000

# number of cores to use for multi-processing
num_cores1 = 64
num_cores2 = 4

###########################################################

def processInput(filename, inputdir, outputdir, num_rows):
    if filename.endswith(".txt"):

        print("Processing file: " + filename)

        infile = inputdir + filename

        # list of outfile that should be skipped if rerun
        skiplist = set()
        skipfiles_name = outputdir + filename.replace(".txt","").replace("CRD3_","") + '_skipfiles.csv'
        if os.path.isfile(skipfiles_name):
            df = pd.read_csv(skipfiles_name, header=None)
            skiplist = set(df[0].unique())

        for word in keywords: 

            mydir = outputdir + word + "/"
            outfile = mydir + filename.replace(".txt","").replace("CRD3_","") + '_' + word + '_address' + '.csv'

            # check if outfile should be skipped
            if outfile in skiplist:
                #print("skipping empty file: " + outfile)
                continue

            # check if outfile already return
            if os.path.isfile(outfile):
                continue
                
            reader = pd.read_csv(infile, 
                             delimiter = '\t', # tab delimiter
                             header=None, # no header in data
                             #dtype = column_types, # take specific types
                             quoting=csv.QUOTE_NONE,
                             chunksize = num_rows, # chunk txt file
                             usecols = [7])

            # indices for rows to read in
            skiprows = []
            nrow_reader = 0

            for df in reader:
                nrow = df.shape[0]
                nrow_reader += nrow

                # whether last name matches keyword
                df = df.loc[~df[7].str.startswith(word, na=False)]
                skiprows += list(df.index.values)

            # if namelist is empty, skip
            if len(skiprows) == nrow_reader:
                #print("namelist is empty: " + outfile)

                with open(skipfiles_name,'a') as skipfiles:
                    skipfiles.write(outfile + '\n')
                continue

            # read in csv, only lines with useful content
            df = pd.read_csv(infile, 
                             delimiter = '\t', # tab delimiter
                             header=None, # no header in data
                             #dtype = column_types, # take specific types
                             quoting=csv.QUOTE_NONE,
                             skiprows=skiprows,
                             usecols = [0,
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
            df.rename(columns={df.columns[0]: "pid"}, 
                      inplace = True)

            # rename columns
            for i in range(1,11):
                col = 1+(i-1)*15
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
                                      "add_state", "add_zip", "add_fips"], i="pid", j="addnum")   

            #print('Memory usage after reshape (in bytes): ' 
            #   + str(df.memory_usage(index=True,deep=True).sum()))

            # drop if missing address id
            df = df[df['add_id'].notnull()]
                
            # make csv
            os.makedirs(mydir, exist_ok=True)
            df.to_csv(outfile, index=True)
            #print(outfile + ' file completed')

def writeOutput(word, workdir, outputdir):
    mydir = workdir + outputdir + word + "/"
    if os.path.exists(mydir):
        path, dirs, files = os.walk(mydir).__next__()
        file_count = len(files)
        if file_count == 1:
            return

        os.chdir(mydir)
        try:
            os.remove(word + "_address.csv") # remove main file
        except OSError:
            pass
        interesting_files = glob.glob("*.csv") 
        try:  
            df_list = pd.concat((pd.read_csv(f, header = 0) for f in interesting_files))
        except:
            return    
        df_list.to_csv(word + "_address.csv", index = False)
        for f in interesting_files:
            os.remove(f)

# main
if __name__ == "__main__":

    # timer
    start = time.time()

    keywords = [''.join(i) for i in product(ascii_uppercase, repeat = 2)]
    #keywords = ['AA','AB']

    with Parallel(n_jobs=num_cores1) as parallel:
        parallel(delayed(processInput)(filename, inputdir, outputdir, num_rows) for filename in os.listdir(inputdir))

    with Parallel(n_jobs=num_cores2) as parallel:
        parallel(delayed(writeOutput)(word, workdir, outputdir) for word in keywords)

    # time
    end = time.time()
    print("Time elapsed (in seconds): " + str(end - start))
