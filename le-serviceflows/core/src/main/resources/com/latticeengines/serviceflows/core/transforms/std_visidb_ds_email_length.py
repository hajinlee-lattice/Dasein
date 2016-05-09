#this is  the email  length, it is a spam indicator

#this needs to be modified
def metadata():
    return {  'ApprovedUsage'   : 'ModelAndAllInsights'
            , 'Category'        : 'Lead Information'
            , 'FundamentalType' : 'numeric'
            , 'Description'     : 'Email length'
            , 'DisplayName'     : 'Email length'
            , 'StatisticalType' : 'ordinal'
            , 'Tags'            : 'Internal' }

def std_visidb_ds_email_length(email): return len(email)

#function to call
def transform(args, record):
    column = args["column"]
    value = record[column]
    return std_visidb_ds_email_length(value)
