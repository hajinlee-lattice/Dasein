import json
import logging
import os
import pickle
import sys
import traceback

import numpy as np
import pandas as pd

logging.basicConfig(filename="scoringengine.log", level=logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='scoringengine')



def debugWrite(logString):
    logger.info(logString)

def decodeDataValue(serializedValueAndType):
    serializedValue = serializedValueAndType.partition('|')[2]
    if len(serializedValue) > 0:
        serializedValue = serializedValue[1:-1]
    else:
        serializedValue = np.NaN
        
    return serializedValue
    
def main(argv):
    currentPath = os.path.dirname(argv[0])
    
    pickleFile = currentPath + '/STPipelineBinary.p'
    
    debugWrite(pickleFile)
       
    pipeline = pickle.load(open(pickleFile, "rb"))
    
    debugWrite("after unpickle")
    
    while 1:
        line = sys.stdin.readline()
        rowDict = getRowToScore(line)
        
        try:
            resultFrame = predict(pipeline, rowDict)
            debugWrite('writing score \r\n')
            debugWrite(str(resultFrame['Score'][0]))
            
            writeLine(resultFrame['Score'][0])
        except Exception as e:
            traceback.print_exc(file=open('exception.log', 'w'))
            debugWrite(str(e) + '\r\n')
            raise

def getRowToScore(line):
    debugWrite(line)
    try:
        decoder = json.decoder.JSONDecoder(encoding='Latin1')
        dataRow = decoder.decode(line)
        debugWrite(str(dataRow))
    except Exception as e:
        debugWrite(str(e))
        raise 
        
    debugWrite('past decoder\r\n')
    rowDict = {}
        
    try:
        for i in dataRow:
            serializedValue = decodeDataValue(i['Value']['SerializedValueAndType'])
            rowDict[i['Key']] = serializedValue
    except Exception as e:
        debugWrite(str(e))
        raise
      
    debugWrite('past rowDict \r\n')
    return rowDict
        
def predict(pipeline, rowDict):
    rowsList = [rowDict]
    dataFrame = pd.DataFrame(rowsList)
    return pipeline.predict(dataFrame)

def writeLine(output):
    print str(output).rstrip('\n')

if __name__ == "__main__":
    sys.exit(main(sys.argv))
