# Method to be used by RTS
def transform(args, record):
    column = args["column"]
    value = record[column]
    return length(value)

# Method to be used in data flow
def length(value):
    return len(value)

# Metadata for this method
def metadata():
    return { "type": "INTEGER", "ApprovedUsage": "Model", "StatisticalType": "ratio" }