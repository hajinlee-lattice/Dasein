#This function looks for words and returns a value if there is a word map
#The function is all based on mapTitleFunctionAny

#required import
from POC10Functions import *

def metadata():
    return {  'ApprovedUsage'   : 'ModelAndAllInsights'
            , 'Category'        : 'Lead Information'
            , 'FundamentalType' : 'nominal'
            , 'StatisticalType' : 'nominal'
            , 'Description'     : 'Title Function'
            , 'DisplayName'     : 'Title Function'                                    
            , 'Tags'            : 'Internal' }

mapTitleFunctionAny=[('IT',set(['it','information technology','database','network','middleware','security'])),
                     ('Engineering',set(['quality','system','engineer','develope','software','testing','unix','linux','product'])),
                     ('Finance',set(['financ','accounting','treasurer','tax','loan','risk','purchasing'])),
                     ('Marketing',set(['market','interactive','advertis','brand','mktg','content','commerc','social','generat','event','media'])),
                     ('Sales',set(['sale','channel','crm','field','inside'])),
                     ('Human Resource',set(['human resource','hr','talent','benefits'])),
                     ('Operations',set(['warehouse','operat','strateg','plan','distribut','chain','facilit'])),
                     ('Public Relations',set(['communication','public','affairs','relation','community','pr'])),
                     ('Design',set(['creative','design','art','designer'])),
                     ('Services',set(['service', 'solution', 'training', 'implementation', 'user', 'help', 'care', 'maintenance', 'engage',
                                     'instruction', 'account manager', 'account executive','soa'])),
                     ('Research',set(['research'])),
                     ('Analytics',set(['data','analy'])),
                     ('Academic',set(['education','academi'])),
                     ('Consulting',set(['consult']))]
 
def std_visidb_ds_title_function(x):
    return valueReturn(x,mapTitleFunctionAny)

#function to call
def transform(args, record):
    column = args["column"]
    value = record[column]
    return std_visidb_ds_title_function(value)
