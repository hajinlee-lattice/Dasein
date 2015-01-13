import base64
import csv
import gzip
import os
from random import shuffle
import shutil
import sys
import uuid

from leframework import scoringengine as se
from leframework.argumentparser import ArgumentParser
import numpy as np
from testbase import TestBase
from testbase import removeFiles


class TrainingTestBase(TestBase):

    def setUp(self):
        # These properties won't really be used since these are just unit tests.
        # Functional and end-to-end tests should be done from java
        os.environ["CONTAINER_ID"] = "xyz"
        os.environ["SHDP_HD_FSWEB"] = "localhost:50070"
        # Simulate what happens in yarn when it copies the framework code over
        # before running the python script
        self.fwkdir = "./leframework.tar.gz"
        self.pipelinefwkdir = "./lepipeline.tar.gz"
        fwkdir = self.fwkdir
        pipelinefwkdir = self.pipelinefwkdir

        if os.path.exists(fwkdir):
            shutil.rmtree(fwkdir)
        if os.path.exists(pipelinefwkdir):
            shutil.rmtree(pipelinefwkdir)

        os.makedirs(fwkdir + "/leframework")
        os.makedirs(pipelinefwkdir)

        enginedir = "/leframework/scoringengine.py"

        os.symlink("../../main/python/pipelinefwk.py", "./pipelinefwk.py")
        os.symlink("../../main/python/pipeline/pipeline.py", "pipeline.py")
        shutil.copy("../../main/python" + enginedir, fwkdir + enginedir)
        shutil.copy("../../main/python/pipeline/encoder.py", pipelinefwkdir + "/encoder.py")
        shutil.copy("../../main/python/pipeline/pipelinesteps.py", pipelinefwkdir + "/pipelinesteps.py")
        sys.path.append(pipelinefwkdir)

        # Symbolic links will be cleaned up by testBase
        scriptDir = "../../main/python/algorithm/" 
        for f in os.listdir(scriptDir):
            fPath = os.path.join(scriptDir, f)
            if os.path.isfile(fPath) and not os.path.exists(f):
                os.symlink(fPath, f)

        results = "./results"
        if os.path.exists(results):
            shutil.rmtree(results)
    
    def tearDown(self):
        if os.path.exists(self.fwkdir):
            shutil.rmtree(self.fwkdir)
        if os.path.exists(self.pipelinefwkdir):
            shutil.rmtree(self.pipelinefwkdir)
        removeFiles(".")
        removeFiles("./results")

    def getLineToScore2(self, inputColumns, typeDict, value):
        columnWithValue = zip(inputColumns, value)
        line = self.getLine(columnWithValue, typeDict)
        return line

    def getLineToScore(self, inputColumns, typeDict, value):
        columnWithValue = zip(inputColumns, value)
        line1 = self.getLine(columnWithValue, typeDict)

        shuffle(columnWithValue)
        line2 = self.getLine(columnWithValue, typeDict)

        return (line1, line2)

    def getLine(self, columnsWithValue, typeDict):
        line = "["
        first = True
        for i in range(len(columnsWithValue)):
            if first:
                first = False
            else:
                line += ","
            if typeDict[columnsWithValue[i][0]] == '-9':
                line += "{\"Key\":\"%s\",\"Value\":{\"SerializedValueAndType\":\"String|'%s'\"}}" % (columnsWithValue[i][0], columnsWithValue[i][1])
            else:
                line += "{\"Key\":\"%s\",\"Value\":{\"SerializedValueAndType\":\"Float|'%s'\"}}" % (columnsWithValue[i][0], columnsWithValue[i][1])
        line += "]"
        line = '{"key":"%s","value":%s}' % (str(uuid.uuid4()), line)
        return line
    
    def createCSV(self, inputColumns, values):
        with open('./results/test.csv', 'wb') as csvfile:
            csvWriter = csv.writer(csvfile)
            csvWriter.writerow(['id'] + inputColumns)
            for i in range(len(values)):
                csvWriter.writerow([i + 1] + values[i])
                
    def stripPath(self, fileName):
        return fileName[fileName.rfind('/') + 1:len(fileName)]
                
    
    def createCSVFromModel(self, modelFile, scoringFile):
        parser = ArgumentParser(modelFile, None)
        schema = parser.getSchema()
        (test, _) = parser.createList(self.stripPath(schema["test_data"]))
        test.reset_index()
        fields = { k['name']:k['type'][0] for k in parser.fields }
        
        with open(scoringFile, "w") as fp:
            for row in test.iterrows():
                line = "["
                first = True
                for field in fields.keys():
                    value = None
                    if field in row[1]:
                        value = row[1][field]
                    else:
                        continue
                    if first:
                        first = False
                    else:
                        line += ","
                    dataType = 'String' if fields[field] == 'string' or fields[field] == 'bytes' else 'Float'
                    if row[1][field] is None or (dataType == 'Float' and np.isnan(row[1][field])):
                        line += "{\"Key\":\"%s\",\"Value\":{\"SerializedValueAndType\":\"%s|\"}}" % (field, dataType)
                    elif dataType == 'String':
                        line += "{\"Key\":\"%s\",\"Value\":{\"SerializedValueAndType\":\"String|'%s'\"}}" % (field, value)
                    else:
                        line += "{\"Key\":\"%s\",\"Value\":{\"SerializedValueAndType\":\"Float|%s\"}}" % (field, str(value))
                line += "]"
                line = '{"key":"%s","value":%s}' % (str(uuid.uuid4()), line)
                fp.write(line + "\n")

    def getPredictScore(self, pipeline, typeDict, values):
        scores = []
        inputColumns = pipeline.getPipeline()[3].getModelInputColumns()
        for value in values:
            row = self.getLine(zip(inputColumns, value), typeDict)
            rowDicts = []
            rowDicts.append(se.getRowToScore(row)[1])
            resultFrame = se.predict(pipeline, rowDicts)
            scores.append(resultFrame['Score'][0])
        return scores

    def decodeBase64ThenDecompressToFile(self, data, filename):
        gzipByteArray = bytearray(base64.decodestring(data))
        with open(filename, "wb") as output:
            output.write(gzipByteArray)

        with gzip.GzipFile(filename, "rb") as compressed:
            data = compressed.read()
            with open(filename, "wb") as decompressed:
                decompressed.write(data)

        return decompressed.name