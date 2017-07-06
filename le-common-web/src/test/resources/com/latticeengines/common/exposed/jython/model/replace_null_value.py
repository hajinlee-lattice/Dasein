imputations = eval(open('imputations.txt', 'r').read())

def transform(args, record):
    column = args["column"]
    return imputations[column]
