pivotValues = eval(open('pivotvalues.txt', 'r').read())

def transform(args, record):
    column = args["column1"]
    targetColumn = args["column2"]
    
    values = pivotValues[targetColumn][1]
    
    for value in values:
        if record[column] == value:
            return 1
    return 0
