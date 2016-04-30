
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

def metadata():
    return {  'ApprovedUsage'   : 'Model'
            , 'DisplayDiscretizationStrategy' : '{"linear": { "minValue":0,"stepSize":1,"minSamples":100,"minFreq":0.01,"maxBuckets":5,"maxPercentile":1}}'
            , 'DisplayName'     : 'Credit Risk Level'
            , 'Category'        : 'Firmographics'
            , 'FundamentalType' : 'numeric'
            , 'StatisticalType' : 'ordinal'
            , 'Tags'            : 'Internal' }


def std_visidb_ds_pd_modelaction_ordered(n):
    if n is None or n == '':
        return None

    valueMap = { 'low risk' : 1
               , 'low-medium risk' : 2
               , 'medium risk' : 3
               , 'medium-high risk' : 4
               , 'high risk' : 5
               , 'recent bankruptcy on file' : 6 }

    if n.lower() in valueMap:
        return valueMap[n.lower()]

    return 0


def transform(args, record):
    column = args["column"]
    value = record[column]
    return std_visidb_ds_pd_modelaction_ordered(value)
