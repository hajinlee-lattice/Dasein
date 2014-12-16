# Matches the behavior of ImputationStep.
def transform(args, record):
    column = args["column"]
    replacement = args["value"]

    value = record[column]
    if value is None:
        value = replacement

    return value
