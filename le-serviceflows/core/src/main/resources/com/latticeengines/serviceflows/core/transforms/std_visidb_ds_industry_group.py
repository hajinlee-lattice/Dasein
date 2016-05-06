
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import re

def metadata():
    return {  'ApprovedUsage'   : 'ModelAndAllInsights'
            , 'Description'     : 'Rollup of Industry field from Marketing Automation'
            , 'DisplayName'     : 'Industry Rollup'
            , 'Category'        : 'Lead Information'
            , 'FundamentalType' : 'alpha'
            , 'StatisticalType' : 'nominal'
            , 'Tags'            : 'InternalTransform' }


def std_visidb_ds_industry_group(n):
    if n is None or n == '':
        return None

    n = n.lower()

    if re.search('credit|financial|bank', n):
        return 'Finance'
    elif re.search('tech|tele', n):
        return 'Tech'
    elif re.search('health|medical|pharm', n):
        return 'Health Care'
    elif re.search('real|property', n):
        return 'Real Estate/Property Mgmt'
    elif re.search('staff|hr', n):
        return 'HR/Staffing'
    elif re.search('services|consulting', n):
        return 'Business Service'
    elif re.search('education', n):
        return 'Education'
    elif re.search('equipment', n):
        return 'Equipment'
    elif re.search('util', n):
        return 'Utilities'
    elif re.search('retail', n):
        return 'Retail'
    elif re.search('transport', n):
        return 'Transportation'
    elif re.search('account|legal', n):
        return 'Accounting/Legal'
    elif re.search('construction', n):
        return 'Construction'
    elif re.search('profit', n):
        return 'Non-Profit'
    elif re.search('insurance', n):
        return 'Insurance'
    elif re.search('manufact', n):
        return 'Manufacturing'

    return 'Other'


def transform(args, record):
    column = args["column"]
    value = record[column]
    return std_visidb_ds_industry_group(value)
