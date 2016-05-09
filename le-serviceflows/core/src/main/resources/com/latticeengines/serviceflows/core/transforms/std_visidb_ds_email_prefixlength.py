#this is  the email prefix length, it is a spam indicator

#this needs to be modified
def metadata():
    return {  'ApprovedUsage'   : 'ModelAndAllInsights'
            , 'Category'        : 'Lead Information'
            , 'FundamentalType' : 'numeric'
            , 'Description'     : 'Email Prefix Length'
            , 'DisplayName'     : 'Email Prefix Length'
            , 'StatisticalType' : 'ordinal'
            , 'Tags'            : 'Internal' }

def std_visidb_ds_email_prefixlength(email): 
	if '@' not in email:
		return 0
	return email.index('@')    

#function to call
def transform(args, record):
    column = args["column"]
    value = record[column]
    return std_visidb_ds_email_prefixlength(value)
