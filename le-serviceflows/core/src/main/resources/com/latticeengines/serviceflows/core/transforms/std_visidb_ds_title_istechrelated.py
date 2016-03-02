
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import re

def metadata():
    return {  'ApprovedUsage'   : 'ModelAndAllInsights'
            , 'Description'     : 'Indicator for Technical Job Title'
            , 'DisplayName'     : 'Has Technical Title'
            , 'Category'        : 'Lead Information'
            , 'StatisticalType' : 'ordinal'
            , 'Tags'            : 'Internal' }


def std_visidb_ds_title_istechrelated(n):
    if n is None:
        return None

    if re.search('(?<!\w)eng|tech|info|dev', n.lower()):
        return True

    return False


def transform(args, record):
    column = args["column"]
    value = record[column]
    return std_visidb_ds_title_istechrelated(value)
