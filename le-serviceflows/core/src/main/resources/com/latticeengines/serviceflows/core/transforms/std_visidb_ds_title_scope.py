#This function looks for words and returns a value if there is a word map
#The function is all based on mapTItleScopeAny

#required import
from POC10Functions import *

def metadata():
    return {  'ApprovedUsage'   : 'ModelAndAllInsights'
            , 'Category'        : 'Lead Information'
            , 'FundamentalType' : 'nominal'
            , 'Description'     : 'Title Scope'
            , 'DisplayName'     : 'Title Scope'                                    
            , 'StatisticalType' : 'nominal'
            , 'Tags'            : 'Internal' }

mapTitleScopeAny=[('Continental',set(['north america','emea','asia','africa','europ','south america'])),
                  ('Global',set(['global','internaltional','worldwide'])),
                  ('National',set(['us','national'])),
                  ('Regional',set(['region','branch','territory','district','central','western','eastern','northern','southern']))]

def std_visidb_ds_title_scope(x):
    return valueReturn(x,mapTitleScopeAny)

#function to call
def transform(args, record):
    column = args["column"]
    value = record[column]
    return std_visidb_ds_title_scope(value)
