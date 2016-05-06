
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

def metadata():
    return {  'ApprovedUsage'   : 'Model'
            , 'Description'     : 'Represents funding stage.  Values are 1 (Startup/Seed), 2 (Early Stage), 3 (Expansion), and 4 (Later Stage)'
            , 'DisplayDiscretizationStrategy' : '{"linear": { "minValue":0,"stepSize":1,"minSamples":100,"minFreq":0.01,"maxBuckets":5,"maxPercentile":1}}'
            , 'DisplayName'     : 'Funding Stage'
            , 'Category'        : 'Growth Trends'
            , 'FundamentalType' : 'numeric'
            , 'StatisticalType' : 'ordinal'
            , 'Tags'            : 'ExternalTransform' }


def std_visidb_ds_pd_fundingstage_ordered(n):
    if n is None or n == '':
        return None

    valueMap = { 'startup/seed' : 1
               , 'early stage' : 2
               , 'expansion' : 3
               , 'later stage' : 4 }

    if n.lower() in valueMap:
        return valueMap[n.lower()]

    return 0


def transform(args, record):
    column = args["column"]
    value = record[column]
    return std_visidb_ds_pd_fundingstage_ordered(value)
