import json
import os
import pickle
import sys
import numpy as np
import pandas as pd
from pipelinefwk import ModelStep
from pipelinefwk import get_logger
import fastavro as avro
from os.path import basename

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

    if len(sys.argv) == 3:
        generateScore(pipeline, inputFileName, outputFileName)
    elif len(sys.argv) == 4 and argv[3] == "avro":
        generateScoreCdl(pipeline, inputFileName, outputFileName)
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
        scoreList = map(lambda x: '{:.6f}'.format(resultFrame[x][index]), headers)
        writeToFile(w, rowIds[index], scoreList)
    w.close()

def generateScoreCdl(pipeline, inputFileName, outputFileName):
    w = open(outputFileName, 'w')
    with open(inputFileName) as f:
        rowIds = []
        rowDicts = []
        reader = avro.reader(f)
        schema = reader.schema
        for line in reader:
            rowId, rowDict = getRowToScoreCdl(line, basename(inputFileName), schema)
            rowIds.append(rowId)
            rowDicts.append(rowDict)
    f.close()
    resultFrame = predict(pipeline, rowDicts)

    headers = ["Score"]

    if not isinstance(pipeline.getPipeline()[-1], ModelStep):
        headers = list(resultFrame.columns.values)
    logger.info("writing score \r\n")


    for index in range(0, len(resultFrame)):
        scoreList = map(lambda x: '{:.6f}'.format(resultFrame[x][index]), headers)
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

def getRowToScoreCdl(dataRow, rowId, schema):
    try:
        decodeDataValueCdl(dataRow, schema["fields"])
    except Exception:
        raise

    return (rowId, dataRow)

def decodeDataValueCdl(dataRow, schemaFields):
    for i in range(0, len(schemaFields)):
        field = schemaFields[i]
        name = field["name"]
        if len(field["type"]) > 0:
            if field["type"][0] != "string" and field["type"][0] != "bytes" and dataRow[name] is not None:
                dataRow[name] = float(dataRow[name])
        elif schema["type"][0] != "string":
            dataRow[name] = None
        else:
            dataRow[name] = np.NaN

def predict(pipeline, rowDict):
    dataFrame = pd.DataFrame(rowDict)
    return pipeline.predict(dataFrame, None, True)

def writeToFile(w, rowId, scoreList):
    w.write(rowId + "," + ",".join(scoreList) + "\n")

if __name__ == "__main__":
    main(sys.argv)
