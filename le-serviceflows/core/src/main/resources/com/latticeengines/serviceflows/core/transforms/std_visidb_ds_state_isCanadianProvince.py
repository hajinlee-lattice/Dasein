#This function looks for words and returns TRUE if there is a word map, FALSE otherwise

def metadata():
    return {  'ApprovedUsage'   : 'ModelAndAllInsights'
            , 'Category'        : 'Lead Information'
            , 'Description'     : 'Region: Canadian Province'
            , 'DisplayName'     : 'Region: Canadian Province'
            , 'FundamentalType' : 'boolean'
            , 'StatisticalType' : 'nominal'
            , 'Tags'            : 'Internal' }

#simple test for membership
def std_visidb_ds_state_isCanadianProvince(x): 
    return x in set(['ON', 'AB', 'NL', 'MB', 'NB', 'BC', 'YT', 'SK', 'QC', 'PE', 'NS', 'NT', 'NU'])

#function to call
def transform(args, record):
    column = args["column"]
    value = record[column]
    return std_visidb_ds_state_isCanadianProvince(value)
