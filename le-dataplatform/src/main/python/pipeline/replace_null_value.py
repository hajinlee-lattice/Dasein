imputations = eval(open('imputations.txt', 'r').read())

def transform(args, record):
    column = args["column"]
    value = record[column]
    if value is None:
        value = imputations[column]
    return value