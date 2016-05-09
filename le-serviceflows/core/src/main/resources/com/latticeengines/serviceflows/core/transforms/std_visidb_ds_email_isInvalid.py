
#this is  the email  length, it is a spam indicator

def metadata():
    return {  'ApprovedUsage'   : 'ModelAndAllInsights'
            , 'Category'        : 'Lead Information'
            , 'FundamentalType' : 'numeric'
            , 'StatisticalType' : 'ordinal'
            , 'Description'     : 'Invalid Email'
            , 'DisplayName'     : 'Invalid Email'
            , 'Tags'            : 'Internal' }

#test to make sure that fits format
def std_visidb_ds_email_isInvalid(email):
    if email is None: return True
    if len(email)<=4: return True
    if '@' not in email: return True
    if email.index('@') in [0,len(email)-1]: return True
    return False

#function to call
def transform(args, record):
    column = args["column"]
    value = record[column]
    return std_visidb_ds_email_isInvalid(value)
