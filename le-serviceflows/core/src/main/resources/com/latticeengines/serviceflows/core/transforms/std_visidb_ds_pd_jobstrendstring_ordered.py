
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

def metadata():
    return {  'ApprovedUsage'   : 'Model'
            , 'Description'     : 'Represents company\'s hiring activity within last 60 days. Values range from 1 (Moderately Hiring) to 3 (Aggressively Hiring)'
            , 'DisplayDiscretizationStrategy' : '{"linear": { "minValue":0,"stepSize":1,"minSamples":100,"minFreq":0.01,"maxBuckets":5,"maxPercentile":1}}'
            , 'DisplayName'     : 'Recent Hiring Activity'
            , 'Category'        : 'Growth Trends'
            , 'StatisticalType' : 'ordinal'
            , 'Tags'            : 'External' }


def std_visidb_ds_pd_jobstrendstring_ordered(n):
    if n is None or n == '':
        return None

    valueMap = { 'moderately hiring' : 1
               , 'significantly hiring' : 2
               , 'aggressively hiring' : 3 }

    if n.lower() in valueMap:
        return valueMap[n.lower()]

    return 0


def transform(args, record):
    column = args["column"]
    value = record[column]
    return std_visidb_ds_pd_jobstrendstring_ordered(value)
