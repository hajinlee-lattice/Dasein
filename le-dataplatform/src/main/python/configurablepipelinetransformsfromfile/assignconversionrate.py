import json
categoricalColumnMapping = json.load(open('conversionratemapping.json', 'rb'))

def transform(args, record):
    column = args["column"]
    value = record[column]
    if column in categoricalColumnMapping:
        if value in categoricalColumnMapping[column]:
            if categoricalColumnMapping[column][value]:
                return categoricalColumnMapping[column][value]
            else:
                return categoricalColumnMapping[column]["UNKNOWN"]

    return value
