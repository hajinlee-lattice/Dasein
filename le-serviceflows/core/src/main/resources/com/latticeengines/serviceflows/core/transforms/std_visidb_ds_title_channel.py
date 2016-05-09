#This function looks for words and returns a value if there is a word map
#The function is all based on mapTitleChannelAny

#required import
from POC10Functions import *

def metadata():
    return {  'ApprovedUsage'   : 'ModelAndAllInsights'
            , 'Category'        : 'Lead Information'
            , 'FundamentalType' : 'nominal'
            , 'StatisticalType' : 'nominal'
            , 'Description'     : 'Title Channel'
            , 'DisplayName'     : 'Title Channel'                                    
            , 'Tags'            : 'Internal' }

mapTitleChannelAny=[('Consumer',set(['consumer','retail'])),
                    ('Government',set(['government'])),
                    ('Corporate',set(['enterprise','corporate']))]

 
def std_visidb_ds_title_channel(x):
    return valueReturn(x,mapTitleChannelAny)

#function to call
def transform(args, record):
    column = args["column"]
    value = record[column]
    return std_visidb_ds_title_channel(value)
