
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import re

def metadata():
    return {  'ApprovedUsage'   : 'ModelAndModelInsights'
            , 'Description'     : 'Numeric score corresponding to Job Title level; Senior Titles have higher values'
            , 'DisplayDiscretizationStrategy' : '{"linear": { "minValue":0,"stepSize":1,"minSamples":100,"minFreq":0.01,"maxBuckets":5,"maxPercentile":1}}'
            , 'DisplayName'     : 'Job Title Seniority'
            , 'Category'        : 'Lead Information'
            , 'StatisticalType' : 'ordinal'
            , 'Tags'            : 'Internal' }



def ds_title_issenior(n):
    if re.search('(?<!\w)sr(?!\w)|senior', n):
        return 1
    return 0

def ds_title_ismanager(n):
    if re.search('(?<!\w)mgr(?!\w)|manag', n):
        return 1
    return 0

def ds_title_isdirector(n):
    if re.search('(?<!\w)dir', n):
        return 1
    return 0

def ds_title_isvpabove(n):
    if re.search('(?<!\w)vp(?!\w)|pres|chief|(?<!\w)c\wo(?!\w)', n):
        return 1
    return 0


def std_visidb_ds_title_level(n):
    if n is None:
        return None

    n = n.lower()

    return ds_title_issenior(n) + (2*ds_title_ismanager(n) + (4*ds_title_isdirector(n) + 8*ds_title_isvpabove(n)))


def transform(args, record):
    column = args["column"]
    value = record[column]
    return std_visidb_ds_title_level(value)
