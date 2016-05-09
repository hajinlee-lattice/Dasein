#This function looks for words and returns TRUE if there is a word map, FALSE otherwise

def metadata():
    return {  'ApprovedUsage'   : 'ModelAndAllInsights'
            , 'Category'        : 'Lead Information'
            , 'FundamentalType' : 'boolean'
            , 'StatisticalType' : 'nominal'
            , 'Description'     : 'Region: Southwest'
            , 'DisplayName'     : 'Region: Southwest'                        
            , 'Tags'            : 'Internal' }

#simple test for membership
def std_visidb_ds_state_isInSouthWest(x):
    return x in set(['AZ', 'OK', 'NM', 'TX'])

#function to call
def transform(args, record):
    column = args["column"]
    value = record[column]
    return std_visidb_ds_state_isInSouthWest(value)
