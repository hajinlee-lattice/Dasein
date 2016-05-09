#This function looks for words and returns TRUE if there is a word map, FALSE otherwise

def metadata():
    return {  'ApprovedUsage'   : 'ModelAndAllInsights'
            , 'Category'        : 'Lead Information'
            , 'FundamentalType' : 'boolean'
            , 'StatisticalType' : 'nominal'
            , 'Description'     : 'Region: Mid-Atlantic'
            , 'DisplayName'     : 'Region: Mid-Atlantic'            
            , 'Tags'            : 'Internal' }


#simple test for membership
def std_visidb_ds_state_isInMidAtlantic(x):
    return x in set(['MD', 'NJ', 'DE', 'DC', 'NY', 'PA'])

#function to call
def transform(args, record):
    column = args["column"]
    value = record[column]
    return std_visidb_ds_state_isInMidAtlantic(value)
