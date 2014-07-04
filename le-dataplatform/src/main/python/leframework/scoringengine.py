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
    serializedType, sep, serializedValue = serializedValueAndType.partition('|')
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

def generateScore(pipeline, inputFileName, outputFileName):
    w = open(outputFileName, 'w')
    with open(inputFileName) as f:
        for line in f:
            rowId, rowDict = getRowToScore(line)
            try:
                resultFrame = predict(pipeline, rowDict)
                logger.info('writing score \r\n')
                logger.info(str(resultFrame['Score'][0]))
                writeToFile(w, rowId, resultFrame['Score'][0])
            except Exception as e:
                w.close()
                raise
    w.close()

def getRowToScore(line):
    logger.info(line)
    try:
        decoder = json.decoder.JSONDecoder(encoding='Latin1')
        dataRow = decoder.decode(line)
        rowId = dataRow['key']
        colValues = dataRow['value']
    except Exception as e:
        raise 
        
    logger.info('past decoder\r\n')
    rowDict = {}
        
    try:
        for i in colValues:
            serializedValue = decodeDataValue(i['Value']['SerializedValueAndType'])
            rowDict[i['Key']] = serializedValue
    except Exception as e:
        raise
      
    logger.info('past rowDict \r\n')
    return (rowId, rowDict)
        
def predict(pipeline, rowDict):
    rowsList = [rowDict]
    dataFrame = pd.DataFrame(rowsList)
    return pipeline.predict(dataFrame)

def writeToFile(w, rowId, score):
    w.write(rowId + "," + str(score) + "\n")

if __name__ == "__main__":
    sys.exit(main(sys.argv))
