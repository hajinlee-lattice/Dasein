pivotValues = eval(open('pivotvalues.txt', 'r').read())

def transform(args, record):
    column = args["column1"]
    targetColumn = args["column2"]
    
    recordValue = record[column]
    if targetColumn.endswith("___ISNULL___"):
        if recordValue is None:
            return 1.0
        return 0.0
    
    values = pivotValues[targetColumn][1]
    
    if values is not None and len(values) > 0:
        for value in values:
            if record[column] == value:
                return 1.0
        return 0.0
    
    return 0.0
