import json
from pandas import isnull
dsTitleImputationsMapping = json.load(open('dstitleimputations.json', 'rb'))
maxTitleLen = dsTitleImputationsMapping['maxTitleLen']
missingValues = dsTitleImputationsMapping['missingValues']

def transform(args, record):

    titleColumn = args['column1']
    title = record[titleColumn]

    titleLengthColumn = args['column2']

    if isnull(title) or title in missingValues:
        if titleLengthColumn in dsTitleImputationsMapping:
            return dsTitleImputationsMapping[titleLengthColumn]
        else:
            return 0.0

    return float(min(len(title), maxTitleLen))
