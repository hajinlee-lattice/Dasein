#This function looks for words and returns TRUE if there is a word map, FALSE otherwise

def metadata():
    return {  'ApprovedUsage'   : 'ModelAndAllInsights'
            , 'Category'        : 'Lead Information'
            , 'FundamentalType' : 'boolean'
            , 'Description'     : 'Region: New England'
            , 'DisplayName'     : 'Region: New England'            
            , 'StatisticalType' : 'nominal'
            , 'Tags'            : 'Internal' }

#simple test for membership
def std_visidb_ds_state_isInNewEngland(x):
    return x in set(['ME', 'NH', 'MA', 'VT', 'RI', 'CT'])

#function to call
def transform(args, record):
    column = args["column"]
    value = record[column]
    return std_visidb_ds_state_isInNewEngland(value)
