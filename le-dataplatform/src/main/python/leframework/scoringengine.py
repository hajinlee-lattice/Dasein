import json
import logging
import os
import pickle
import sys
import numpy as np
import pandas as pd

logger = logging.getLogger("scoringengine")
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

def decodeDataValue(serializedValueAndType):
    serializedType, _, serializedValue = serializedValueAndType.partition('|')
    if len(serializedValue) > 0:
        serializedValue = serializedValue[1:-1]
    elif serializedType == "String":
        serializedValue = None
    else:
        serializedValue = np.NaN
        
    return serializedValue
    
def main(argv):
    currentPath = os.path.dirname(argv[0])
    inputFileName = argv[1]
    outputFileName = argv[2]
    
    pickleFile = currentPath + '/STPipelineBinary.p'
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
    logger.info('writing score \r\n')
    for index in range(0, len(resultFrame)):
        writeToFile(w, rowIds[index], resultFrame['Score'][index])
    w.close()

def getRowToScore(line):
    try:
        decoder = json.decoder.JSONDecoder(encoding='Latin1')
        dataRow = decoder.decode(line)
        rowId = dataRow['key']
        colValues = dataRow['value']
    except Exception:
        raise

    rowDict = {}

    try:
        for i in colValues:
            serializedValue = decodeDataValue(i['Value']['SerializedValueAndType'])
            rowDict[i['Key']] = serializedValue
    except Exception:
        raise

    return (rowId, rowDict)

def predict(pipeline, rowDict):
    dataFrame = pd.DataFrame(rowDict)
    return pipeline.predict(dataFrame)

def writeToFile(w, rowId, score):
    w.write(rowId + "," + str(score) + "\n")

if __name__ == "__main__":
    sys.exit(main(sys.argv))
