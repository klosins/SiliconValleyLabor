# Rose Tan
# Date created: 1/31/2018
# Last edited: 2/13/2018
#
# Description: 	This file takes in a csv file of cleaned infutor data from read_txt_v2.py
# 				and writes it to a SQL database
# 
# Steps: 	1. Download MySQL, SequelPro, and PyMySQL 
# 			2. Run MySQL in the background: sudo launchctl load -F /Library/LaunchDaemons/com.oracle.oss.mysql.mysqld.plist
# 			3. Run this file
#
# TO DO: 	add PHone and Alias; currently only works for PersonAddress!!


# libraries
import peewee # for connecting to SQL
import pandas as pd
import numpy as np
import time # timer
import os # for bash commands

#################### SET PARAMETERS #######################

# set directory
os.chdir("/home/zqian/Documents/mysql_infutor")

# infile name or path
infile_address = 'NE_address1.csv'


# connection parameters for SQL database
cnx = {
	'NAME': 'infutor',
	'USER': 'root',
	'PASSWORD': 'Pakage34',
	'HOST': '127.0.0.1'
}

# connect to SQL database
db = peewee.MySQLDatabase(cnx['NAME'], host=cnx['HOST'], port=3306, user=cnx['USER'], passwd=cnx['PASSWORD'])


######################################@#################### 



# make sure data has all columns
def boost_it(list_of_dicts, peewee_model):
	
	# list of all possible keys
	if peewee_model == PersonAddress:
		master_keys = ['pid', 'addnum', 'name_last', 'name_middle', 'name_prefix', 'name_suffix', 
		'id_primary', 'deceased', 'gender', 'date_last', 'dob', 'id_primary_seq', 'name_first', 
		'id_second', 'date_orig', 'add_id', 'date_eff', 'date_beg', 'date_end', 'add', 'add_stnum', 
		'add_stpre', 'add_stname', 'add_sttype', 'add_stsuf', 'add_aptnum', 'add_city', 'add_state',
		'add_zip', 'add_fips']
	elif peewee_model == PersonAlias:
		master_keys = ['pid', 'alias_num', 'id_primary', 'id_primary_seq', 'id_second', 'name_prefix', 
		'name_first', 'name_middle','name_last', 'name_suffix', 'gender', 'dob', 'alias', 'alias_prefix',
		'alias_first', 'alias_middle', 'alias_last', 'alias_suffix', 'alias_gender', 'dead'] 
	elif peewee_model == PersonPhone: 
		master_keys = ['pid', 'phone_num', 'id_primary', 'id_primary_seq', 'id_second',
		'name_prefix','name_first', 'name_middle', 'name_last', 'name_suffix', 'gender', 'dob', 'phone', 
		'phone_internal1', 'phone_internal2','phone_internal3','phone_internal4','dead',
		'phone_month_orig','phone_month_last']
	
	# loop over all entries
	for dictnum in range(len(list_of_dicts)):
		# loop over all possible keys
		for i in master_keys:
			# if master key doesn't exist
			if list_of_dicts[dictnum].get(i) == None:
				# create the key and assign null as value
				list_of_dicts[dictnum][i] = None
	# return the list of dicts
	for x in list_of_dicts:
		yield x




# splits database into smaller pieces (too many entries will kill it)
def chunker(seq, size):
	return (seq[pos:pos + size] for pos in range(0, len(seq), size))



# insert data (dictionary format) into SQL database
def insert_to_peewee(list_of_dicts, db, peewee_model, func, multi=False, upsert=True):

	if multi:
		db.get_conn()

	if len(list_of_dicts) > 4000:
		for group in chunker(list_of_dicts, 2000):
			with db:
				peewee_model.insert_many(boost_it(group, peewee_model)).execute()
	else:
		with db:
			peewee_model.insert_many(boost_it(list_of_dicts, peewee_model)).execute()

	if multi:
		db.close()




# variables in address table
class PersonAddress(peewee.Model):

	pid = peewee.IntegerField(null=True)
	addnum = peewee.IntegerField(null=True)
	date_last = peewee.IntegerField(null=True)
	date_orig = peewee.IntegerField(null=True)
	date_eff = peewee.IntegerField(null=True)
	date_beg = peewee.IntegerField(null=True)
	date_end = peewee.IntegerField(null=True)
	id_primary = peewee.FloatField(null=True)
	id_primary_seq = peewee.FloatField(null=True)
	id_second = peewee.FloatField(null=True)
	add_id = peewee.IntegerField(null=True)
	add = peewee.TextField(null=True) 
	add_stnum = peewee.TextField(null=True) 
	add_stpre = peewee.TextField(null=True) 
	add_stname = peewee.TextField(null=True) 
	add_sttype = peewee.TextField(null=True) 
	add_stsuf = peewee.TextField(null=True) 
	add_aptnum = peewee.TextField(null=True) 
	add_city = peewee.TextField(null=True) 
	add_state = peewee.TextField(null=True) 
	add_zip = peewee.TextField(null=True) 
	add_fips = peewee.IntegerField(null=True)
	name_prefix = peewee.TextField(null=True) 
	name_first = peewee.TextField(null=True) 
	name_middle = peewee.TextField(null=True) 
	name_last = peewee.TextField(null=True) 
	name_suffix = peewee.TextField(null=True) 
	gender = peewee.TextField(null=True) 
	dob = peewee.IntegerField(null=True)
	deceased = peewee.TextField(null=True)


	class Meta:
		database = db




# variables in Alias table
class PersonAlias(peewee.Model):

	pid = peewee.IntegerField(null=True)
	alias_num = peewee.IntegerField(null=True)
	id_primary = peewee.FloatField(null=True)
	id_primary_seq = peewee.FloatField(null=True)
	id_second = peewee.FloatField(null=True)
	name_prefix = peewee.TextField(null=True) 
	name_first = peewee.TextField(null=True) 
	name_middle = peewee.TextField(null=True) 
	name_last = peewee.TextField(null=True) 
	name_suffix = peewee.TextField(null=True) 
	gender = peewee.TextField(null=True) 
	dob = peewee.IntegerField(null=True)
	alias = peewee.TextField(null=True)
	alias_prefix = peewee.TextField(null=True)
	alias_first = peewee.TextField(null=True)
	alias_middle = peewee.TextField(null=True)
	alias_last = peewee.TextField(null=True)
	alias_suffix = peewee.TextField(null=True)
	alias_gender = peewee.TextField(null=True)
	dead = peewee.IntegerField(null=True)

	class Meta:
		database = db




# variables in Phone table
class PersonPhone(peewee.Model):

	pid = peewee.IntegerField(null=True)
	phone_num =  peewee.IntegerField(null=True)
	id_primary = peewee.FloatField(null=True)
	id_primary_seq = peewee.FloatField(null=True)
	id_second = peewee.FloatField(null=True)
	name_prefix = peewee.TextField(null=True) 
	name_first = peewee.TextField(null=True) 
	name_middle = peewee.TextField(null=True) 
	name_last = peewee.TextField(null=True) 
	name_suffix = peewee.TextField(null=True) 
	gender = peewee.TextField(null=True) 
	dob = peewee.IntegerField(null=True)
	phone =  peewee.BigIntegerField(null=True)
	phone_internal1 = peewee.TextField(null=True)
	phone_internal2 = peewee.TextField(null=True)
	phone_internal3 = peewee.TextField(null=True)
	phone_internal4 = peewee.TextField(null=True)
	phone_month_orig = peewee.TextField(null=True)
	phone_month_last = peewee.TextField(null=True)
	dead = peewee.IntegerField(null=True)

	class Meta:
		database = db




# drop the NAs
def to_dict_dropna(df):
	return [ {k:v for k,v in m.items() if pd.notnull(v)} for m in df.to_dict(orient='rows')]
	#return [v.dropna().to_dict() for k,v in df.iterrows()] # equivalent but slower




# main
if __name__ == "__main__":

	# timer
	start = time.time()


	# tables
	tables = [PersonAddress,PersonPhone,PersonAlias]


	db.drop_tables(tables) # drop table
	db.create_tables([PersonAddress,PersonPhone,PersonAlias]) # add table


	# loop over each table
	for each_table in tables:

		# set file path for each table
		if each_table == PersonAlias:
			path = 'WY1_perc_name.csv'
		elif each_table == PersonPhone:
			path = 'WY1_perc_phone.csv'
		elif each_table == PersonAddress:
			path = infile_address


		# import from csv to dataframe
		df = pd.read_csv(path)


		# drop the NAs and convert to dictionary (must drop NAs or else will get error: 
		# 							ValueError: cannot convert float NaN to integer)
		df_dict = to_dict_dropna(df)

		# uncomment to print the master keys
		#df_dict_keys = df_dict[1].keys()
		#print(df_dict_keys) 


		# write to table
		insert_to_peewee(df_dict, db, each_table, boost_it) 


	# time
	end = time.time()
	print("Time elapsed (in seconds): " + str(end - start))