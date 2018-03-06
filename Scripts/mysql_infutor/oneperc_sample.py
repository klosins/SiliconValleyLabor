# Rose Wang Tan
# Date created: 10/30/2017
# Date last edited: 10/31/2017
#
# Description: Creates 1 percent sample of Infutor migration data
#             1. create a subdirectory within the input directory called "outfiles" for output


import os
import random
import math
import string
import time

# set directory where all input files are stored
directory = '/media/zqian/Seagate Backup Plus Drive/CRD3/raw'

# change directory
os.chdir(directory)

# track time
start_time = time.time()

# set random seed
random.seed(1)

# determine the file length
def file_len(fname):
    with open(fname) as f:
        numline = sum(1 for _ in f) # count number of lines
        oneperc = int(math.ceil(0.01*numline)) # compute 1% of lines
        return oneperc
    
# draw the sample
def draw_sample(fname,number):
    with open(fname) as f:
        lines = random.sample(f.readlines(),number)
        return lines

    
for filename in os.listdir(directory):
    if filename.endswith(".txt"): 
        length = file_len(filename)
        sample = draw_sample(filename,length)
        sample_str = " ".join(sample)
    
        filename_sub = filename.strip('.txt')
        file = open('outfiles/' + filename_sub + '1perc' + '.txt', 'w')
        file.write(sample_str) 
        file.close()
        
        print('done with: ' + filename)

# compute time
elapsed_time = time.time() - start_time
print('elapsed time: ' + str(elapsed_time) + ' seconds')
