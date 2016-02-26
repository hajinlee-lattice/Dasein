
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

def metadata():
    return {  'ApprovedUsage'   : 'Model'
            , 'DisplayDiscretizationStrategy' : '{"geometric": { "minValue":1,"multiplierList":[2,2.5,2],"minSamples":100,"minFreq":0.01,"maxBuckets":5,"maxPercentile":1}}'
            , 'Category'        : 'Online Presence'
            , 'FundamentalType' : 'numeric'
            , 'StatisticalType' : 'ratio'
            , 'Tags'            : 'External' }


def std_visidb_ds_pd_alexa_relatedlinks_count(n):
    if n is None or n == '':
        return None
    return n.count(',') + 1


def transform(args, record):
    column = args["column"]
    value = record[column]
    return std_visidb_ds_pd_alexa_relatedlinks_count(value)
