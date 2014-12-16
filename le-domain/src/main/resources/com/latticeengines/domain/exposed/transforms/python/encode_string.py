import math

# Taken from encoder.py; should reduce this to a single source.
def encode(x):
    if x is None:
        x = 'NULL'
        
    if isinstance(x, (int, long, float)):
        if math.isnan(x):
            x = 'NULL'
        else:
            return x
    try:
        return int(0xffffffff & reduce(lambda h,c: ord(c) + (h << 6) + (h << 16) - h, x, 0))
    except Exception:
        print("Error with type = %s and value = %s" % (type(x), x))
        raise

# Matches the behavior of EnumeratedColumnTransformStep.
def transform(args, record):
    column = args["column"]
    value = record[column]

    return int(encode(value))
