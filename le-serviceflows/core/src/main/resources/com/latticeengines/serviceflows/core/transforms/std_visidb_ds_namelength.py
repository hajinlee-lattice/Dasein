#this is the domain length, it is a spam indicator

#this needs to be modified
def metadata():
    return {  'ApprovedUsage'   : 'ModelAndAllInsights'
            , 'Category'        : 'Lead Information'
            , 'FundamentalType' : 'numeric'
            , 'Description'     : 'Name Length'
            , 'DisplayName'     : 'Name Length'
            , 'StatisticalType' : 'ordinal'
            , 'Tags'            : 'Internal' }

def std_visidb_ds_namelength(FirstName,LastName):
    return len(FirstName)+len(LastName)

#function to call
def transform(args, record):
    column1 = args["column1"]
    column2 = args["column2"]
    value1 = record[column1]
    value2 = record[column2]
    return std_visidb_ds_namelength(value1, value2)
