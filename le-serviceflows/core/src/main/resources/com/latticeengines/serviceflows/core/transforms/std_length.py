
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

def metadata():
    return {  'ApprovedUsage'   : 'Model'
            , 'DisplayDiscretizationStrategy' : '{"linear": { "minValue":0,"stepSize":1,"minSamples":100,"minFreq":0.01,"maxBuckets":5,"maxPercentile":1}}'
            , 'Category'        : 'Lead Information'
            , 'FundamentalType' : 'numeric'
            , 'StatisticalType' : 'ratio'
            , 'Tags'            : 'Internal' }


def std_length(n):
    if n is None or n == '':
        return 1
    if len(n) > 30:
        return 30
    return len(n)


def transform(args, record):
    column = args["column"]
    value = record[column]
    return std_length(value)
