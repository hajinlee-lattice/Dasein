#This function looks for words and returns TRUE if there is a word map, FALSE otherwise

def metadata():
    return {  'ApprovedUsage'   : 'ModelAndAllInsights'
            , 'Category'        : 'Lead Information'
            , 'FundamentalType' : 'boolean'
            , 'Description'     : 'Region: Rocky Mountain'
            , 'DisplayName'     : 'Region: Rocky Mountain'                        
            , 'StatisticalType' : 'nominal'
            , 'Tags'            : 'Internal' }

#simple test for membership
def std_visidb_ds_state_isInRockyMountains(x):
    return x in (['MT', 'CO', 'ID', 'WY', 'UT'])

#function to call
def transform(args, record):
    column = args["column"]
    value = record[column]
    return std_visidb_ds_state_isInRockyMountains(value)
