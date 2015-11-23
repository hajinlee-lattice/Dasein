import json
import os
import pickle
import sys
import numpy as np
import pandas as pd
from pipelinefwk import ModelStep
from pipelinefwk import get_logger
 
logger = get_logger("scoringengine")
 
def decodeDataValue(serializedValueAndType):
    serializedType, _, serializedValue = serializedValueAndType.partition('|')
    if len(serializedValue) > 0:
        serializedValue = serializedValue[1:-1]
        if serializedType == 'Float':
            serializedValue = float(serializedValue)
    elif serializedType == "String":
        serializedValue = None
    else:
        serializedValue = np.NaN
         
    return serializedValue
     
def main(argv):
    currentPath = os.path.dirname(argv[0])
    inputFileName = argv[1]
    outputFileName = argv[2]
     
    pickleFile = os.path.join(currentPath, "STPipelineBinary.p")
    logger.info(pickleFile)
        
    pipeline = pickle.load(open(pickleFile, "rb"))
    logger.info("after unpickle")
     
    generateScore(pipeline, inputFileName, outputFileName)
    logger.info("scoring complete")
 
def generateScore(pipeline, inputFileName, outputFileName):
    w = open(outputFileName, 'w')
    with open(inputFileName) as f:
        rowIds = []
        rowDicts = []
        for line in f:
            rowId, rowDict = getRowToScore(line)
            rowIds.append(rowId)
            rowDicts.append(rowDict)
    f.close()
    resultFrame = predict(pipeline, rowDicts)
     
    headers = ["Score"]
     
    if not isinstance(pipeline.getPipeline()[-1], ModelStep):
        headers = list(resultFrame.columns.values)
    logger.info("writing score \r\n")
     
     
    for index in range(0, len(resultFrame)):
        scoreList = map(lambda x: str(resultFrame[x][index]), headers)
        writeToFile(w, rowIds[index], scoreList)
    w.close()
 
def getRowToScore(line):
    try:
        decoder = json.decoder.JSONDecoder(encoding="utf-8")
        dataRow = decoder.decode(line)
        rowId = dataRow["key"]
        colValues = dataRow["value"]
    except Exception:
        raise
 
    rowDict = {}
 
    try:
        for i in colValues:
            serializedValue = decodeDataValue(i["Value"]["SerializedValueAndType"])
            rowDict[i["Key"]] = serializedValue
    except Exception:
        raise
 
    return (rowId, rowDict)
 
def predict(pipeline, rowDict):
    dataFrame = pd.DataFrame(rowDict)
    targetColumn = pipeline.getPipeline()[2].targetColumn_
 
    if targetColumn in dataFrame.columns.values:
        dataFrame.drop(targetColumn, axis = 1, inplace = True)
    return pipeline.predict(dataFrame, False)
 
def writeToFile(w, rowId, scoreList):
    w.write(rowId + "," + ",".join(scoreList) + "\n")
 
if __name__ == "__main__":
    sys.exit(main(sys.argv))