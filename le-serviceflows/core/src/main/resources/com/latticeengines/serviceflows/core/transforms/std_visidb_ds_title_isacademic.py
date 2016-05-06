
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import re
from std_visidb_ds_title_level import std_visidb_ds_title_level

def metadata():
    return {  'ApprovedUsage'   : 'ModelAndAllInsights'
            , 'Description'     : 'Indicator for Academic Job Title'
            , 'DisplayName'     : 'Has Academic Title'
            , 'Category'        : 'Lead Information'
            , 'FundamentalType' : 'boolean'
            , 'StatisticalType' : 'nominal'
            , 'Tags'            : 'InternalTransform' }


def std_visidb_ds_title_isacademic(n):
    if n is None:
        return False

    if re.search('student|researcher|professor|dev|programmer', n.lower()) and std_visidb_ds_title_level(n) == 0:
        return True

    return False


def transform(args, record):
    column = args["column"]
    value = record[column]
    return std_visidb_ds_title_isacademic(value)
