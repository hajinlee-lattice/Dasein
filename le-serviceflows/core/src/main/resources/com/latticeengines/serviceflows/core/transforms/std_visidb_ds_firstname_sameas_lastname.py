
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

def metadata():
    return {  'ApprovedUsage'   : 'Model'
            , 'Description'     : 'Indicates that First Name is same as Last Name'
            , 'DisplayName'     : 'Identical First and Last Names'
            , 'Category'        : 'Lead Information'
            , 'FundamentalType' : 'boolean'
            , 'StatisticalType' : 'nominal'
            , 'Tags'            : 'Internal' }


def std_visidb_ds_firstname_sameas_lastname(firstname, lastname):
    if firstname is None or lastname is None:
        return False

    if firstname == '' and lastname == '':
        return False

    if firstname.lower() == lastname.lower():
        return True

    return False


def transform(args, record):
    column1 = args["column1"]
    column2 = args["column2"]
    firstname = record[column1]
    lastname = record[column2]
    return std_visidb_ds_firstname_sameas_lastname(firstname, lastname)
