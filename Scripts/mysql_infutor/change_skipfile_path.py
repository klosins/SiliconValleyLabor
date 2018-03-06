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

# set directory
#workdir = "/media/zqian/Seagate Backup Plus Drive/CRD3/phone_csv/"
workdir = "/ifs/gsb/tmcquade/BDMProject/SiliconValleyLabor/Data/phone_csv/"
#workdir = "/media/zqian/Seagate Backup Plus Drive/infutor_1perc/data/phone_csv/"
os.chdir(workdir)

# main
if __name__ == "__main__":

    # timer
    start = time.time()

    for skipfiles_name in glob.glob("*.csv"):
        df = pd.read_csv(skipfiles_name, header=None)
        df[0] = df[0].str.replace('address','phone')
        df.to_csv(skipfiles_name, index = False, header=None)

    # time
    end = time.time()
    print("Time elapsed (in seconds): " + str(end - start))