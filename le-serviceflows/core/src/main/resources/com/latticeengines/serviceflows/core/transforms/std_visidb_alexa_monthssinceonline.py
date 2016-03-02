
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import datetime

def metadata():
    return {  'ApprovedUsage'         : 'ModelAndModelInsights'
            , 'DataType'              : 'Integer'
            , 'Description'           : 'Number of months since online presence was established'
            , 'DisplayDiscretization' : '{"geometric": { "minValue":1,"multiplierList":[2,2.5,2],"minSamples":100,"minFreq":0.01,"maxBuckets":5,"maxPercentile":1}}'
            , 'DisplayName'           : 'Months Since Online'
            , 'Category'              : 'Online Presence'
            , 'FundamentalType'       : 'numeric'
            , 'StatisticalType'       : 'ratio'
            , 'Tags'                  : 'External' }


def std_visidb_alexa_monthssinceonline(date):

    if date is None or date == '':
        return None

    if isinstance(date, long):
        dt = datetime.datetime.fromtimestamp(date / 1000)
    else:
        dt = datetime.datetime.strptime(date, '%m/%d/%Y %I:%M:%S %p')

    return int(round((datetime.datetime.now() - dt).total_seconds())) / 2626560


def transform(args, record):
    column = args["column"]
    value = record[column]
    return std_visidb_alexa_monthssinceonline(value)
