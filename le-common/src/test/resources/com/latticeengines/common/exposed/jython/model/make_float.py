def transform(args, record):
    column = args["column"]
    value = record[column]

    if value is None:
        return None
    return tofloat(value)

def tofloat(value):
    return float(value)