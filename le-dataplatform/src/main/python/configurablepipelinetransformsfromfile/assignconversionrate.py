import json
import numbers

categoricalColumnMapping = json.load(open('conversionratemapping.json', 'rb'))

def transform(args, record):
    column = args["column"]
    value = record[column]
    
    if isinstance(value, numbers.Number):
        value = format(value, ".2f")
    if column in categoricalColumnMapping:
        if value is None or value == 'nan':
            return categoricalColumnMapping[column]["UNKNOWN"]
        if value in categoricalColumnMapping[column]:
            if categoricalColumnMapping[column][value]:
                return categoricalColumnMapping[column][value]
            else:
                return categoricalColumnMapping[column]["UNKNOWN"]

    return value
