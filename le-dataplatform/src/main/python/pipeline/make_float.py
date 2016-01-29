# Matches the behavior of ColumnTypeConversionStep.
def transform(args, record):
    column = args["column"]
    value = record[column]

    if value is None:
        return None
        
    return float(value)
