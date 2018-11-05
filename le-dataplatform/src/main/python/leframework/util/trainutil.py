from pipelinefwk import get_logger

logger = get_logger("algorithm")

def getDisplayName(displayNames, columnName):
    displayName = columnName
    if columnName in displayNames:
        displayName = displayNames[columnName]
    return '"'+ displayName.replace('"', '""') + '"'

def createDisplayNames(schema):
    result = dict()
    configMetadata = schema["config_metadata"]
    if configMetadata is not None and "Metadata" in configMetadata:
        for element in configMetadata["Metadata"]:
            if "DisplayName" in element and element["DisplayName"] is not None and element["DisplayName"] != '': 
                result[element["ColumnName"]] = element["DisplayName"]
            else:
                result[element["ColumnName"]] = element["ColumnName"]
    return result            

