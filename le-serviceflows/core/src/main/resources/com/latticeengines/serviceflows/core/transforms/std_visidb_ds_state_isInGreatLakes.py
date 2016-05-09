#This function looks for words and returns TRUE if there is a word map, FALSE otherwise

def metadata():
    return {  'ApprovedUsage'   : 'ModelAndAllInsights'
            , 'Category'        : 'Lead Information'
            , 'Description'     : 'Region: Great Lakes'
            , 'DisplayName'     : 'Region: Great Lakes'            
            , 'FundamentalType' : 'boolean'
            , 'StatisticalType' : 'nominal'
            , 'Tags'            : 'Internal' }

#simple test for membership
def std_visidb_ds_state_isInGreatLakes(x):
    return x in set(['OH', 'MI', 'IL', 'WI', 'IN'])

#function to call
def transform(args, record):
    column = args["column"]
    value = record[column]
    return std_visidb_ds_state_isInGreatLakes(value)
