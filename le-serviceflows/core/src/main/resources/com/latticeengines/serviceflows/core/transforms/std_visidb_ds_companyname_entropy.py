
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import math

def metadata():
    return {  'ApprovedUsage'   : 'Model'
            , 'Category'        : 'Lead Information'
            , 'FundamentalType' : 'numeric'
            , 'StatisticalType' : 'ratio'
            , 'Tags'            : 'InternalTransform' }


def argument_length(n):
    if n is None:
        return 1
    if len(n) > 30:
        return 30
    return len(n)


def std_visidb_ds_companyname_entropy(s):
    if s is None or argument_length(s) == 0:
        return None

    s = s.lower()
    base = 2

    occurences = {}
    for c in s:
        if c in occurences:
            occurences[c] += 1
        else:
            occurences[c] = 1
    if len(occurences) < 2:
        return 0.0
    e = 0.0
    depth = float(len(s))
    for (k,v) in occurences.iteritems():
        p = float(v)/depth
        e -= p * math.log(p, base)

    return e / float(argument_length(s))


def transform(args, record):
    column = args["column"]
    value = record[column]
    return std_visidb_ds_companyname_entropy(value)
