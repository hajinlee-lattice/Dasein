#This function looks for words and returns TRUE if there is a word map, FALSE otherwise

def metadata():
    return {  'ApprovedUsage'   : 'ModelAndAllInsights'
            , 'Category'        : 'Lead Information'
            , 'FundamentalType' : 'boolean'
            , 'StatisticalType' : 'nominal'
            , 'Description'     : 'Region: Southeast'
            , 'DisplayName'     : 'Region: Southeast'                        
            , 'Tags'            : 'Internal' }


#simple test for membership
def std_visidb_ds_state_isInSouthEast(x):
    return x in set(['VA', 'MS', 'LA', 'NC', 'AL', 'TN', 'WV', 'AR', 'GA', 'SC', 'KY', 'FL'])
 

#function to call
def transform(args, record):
    column = args["column"]
    value = record[column]
    return std_visidb_ds_state_isInSouthEast(value)
