#This function looks for words and returns a value if there is a word map
#The function is all based on mapTitleRoleAny

#required import
from POC10Functions import *

def metadata():
    return {  'ApprovedUsage'   : 'ModelAndAllInsights'
            , 'Category'        : 'Lead Information'
            , 'FundamentalType' : 'nominal'
            , 'Description'     : 'Title Role'
            , 'DisplayName'     : 'Title Role'                                    
            , 'StatisticalType' : 'nominal'
            , 'Tags'            : 'Internal' }

mapTitleRoleAny=[('Associate', set(['assoc'])),
              ('Assistant', set(['secret', 'assist'])),
              ('Leadership', set(['founder', 'vice', 'vp', 'evp', 'chief', 'owner', 'president', 'svp','ceo','cfo','cto','cio'])),
              ('Manager', set(['mgr', 'gm', 'supervisor', 'manag', 'lead'])),
              ('Director', set(['director', 'dir'])),
              ('Engineer', set(['programmer', 'developer', 'dev', 'engineer'])),
              ('Consultant', set(['consultant'])),
              ('Student_Teacher', set(['instructor', 'coach', 'teacher', 'student', 'faculty'])),
              ('Analyst', set(['analyst'])),
              ('Admin', set(['admin', 'dba'])),
              ('Investor_Partner', set(['partner', 'investor', 'board'])),
              ('Controller_Accountant', set(['controller', 'accountant'])),
              ('Specialist_Technician', set(['technician', 'specialist'])),
              ('Architect', set(['architect'])),
              ('Representative', set(['representative'])),
              ('Editor', set(['editor']))]

def std_visidb_ds_title_role(x):
    return valueReturn(x,mapTitleRoleAny)

#function to call
def transform(args, record):
    column = args["column"]
    value = record[column]
    return std_visidb_ds_title_role(value)
