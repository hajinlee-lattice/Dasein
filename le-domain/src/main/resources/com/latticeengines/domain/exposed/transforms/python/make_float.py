# Matches the behavior of ColumnTypeConversionStep.
def transform(args, record):
    column = args["column"]
    value = record[column]

    return float(value)
