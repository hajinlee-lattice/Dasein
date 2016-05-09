#This function looks for words and returns a value if there is a word map

from POC10Functions import *

def metadata():
    return {  'ApprovedUsage'   : 'ModelAndAllInsights'
            , 'Category'        : 'Lead Information'
            , 'FundamentalType' : 'nominal'
            , 'StatisticalType' : 'nominal'
            , 'Description'     : 'Title Category'
            , 'DisplayName'     : 'Title Category'                                    
            , 'Tags'            : 'Internal' }

def std_visidb_ds_title_level_categorical(x):
    if DS_HasUnusualChar(x): return ''
    y=x.lower()
    if 'vice' in y: return 'Vice President'
    if 'director' in y: return 'Director'
    if 'manager' in y:  return 'Manager'
    return 'Staff'

#function to call
def transform(args, record):
    column = args["column"]
    value = record[column]
    return std_visidb_ds_title_level_categorical(value)
