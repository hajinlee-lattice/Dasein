import base64
import csv
import filecmp
import gzip
import json
import os
import sys
import pickle
from random import random
from random import shuffle
import shutil
from sklearn.ensemble import RandomForestClassifier
import uuid

from testbase import TestBase
from leframework import scoringengine as se

class LauncherTest(TestBase):

    def setUp(self):
        # Simulate what happens in yarn when it copies the framework code over
        # before running the python script
        fwkdir = "./leframework.tar.gz"
        if os.path.exists(fwkdir):
            shutil.rmtree(fwkdir)

        os.makedirs(fwkdir + "/leframework")
        enginedir = "/leframework/scoringengine.py"
        shutil.copyfile("../../main/python" + enginedir, fwkdir + enginedir)
        shutil.copyfile("../../main/python/pipeline.py", fwkdir + "/pipeline.py")
        shutil.copyfile("../../main/python/encoder.py", fwkdir + "/encoder.py")

        # Symbolic links will be cleaned up by testBase
        scriptDir = "../../main/python/algorithm/" 
        for f in os.listdir(scriptDir):
            fPath = os.path.join(scriptDir,f)
            if os.path.isfile(fPath) and not os.path.exists(f):
                os.symlink(fPath, f)

        results = "./results"
        if os.path.exists(results):
            shutil.rmtree(results)

    def testExecuteLearning(self):
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher

        # These properties won't really be used since these are just unit tests.
        # Functional and end-to-end tests should be done from java
        os.environ["CONTAINER_ID"] = "xyz"
        os.environ["SHDP_HD_FSWEB"] = "localhost:50070"
        traininglauncher = Launcher("model.json")
        traininglauncher.execute(False)

        # Retrieve the pickled model from the json file
        jsonDict = json.loads(open("./results/model.json").read())

        payload = "./results/STPipelineBinary.p.gz"
        self.__decodeBase64ThenDecompressToFile(jsonDict["Model"]["CompressedSupportFiles"][2]["Value"], payload)
        # Load from the file system and deserialize into the model
        pipeline = pickle.load(open(payload + ".decompressed", "r"))
        self.assertTrue(isinstance(pipeline.getPipeline()[2].getModel(), RandomForestClassifier), "clf not instance of sklearn RandomForestClassifier.")

        pipelineScript = "./results/pipeline.py.gz"
        self.__decodeBase64ThenDecompressToFile(jsonDict["Model"]["CompressedSupportFiles"][1]["Value"], pipelineScript)
        self.assertTrue(filecmp.cmp(pipelineScript + ".decompressed", './leframework.tar.gz/pipeline.py'))

        encoderScript = "./results/encoder.py.gz"
        self.__decodeBase64ThenDecompressToFile(jsonDict["Model"]["CompressedSupportFiles"][0]["Value"], encoderScript)
        self.assertTrue(filecmp.cmp(encoderScript + ".decompressed", './leframework.tar.gz/encoder.py'))

        self.assertTrue(jsonDict["Model"]["Script"] is not None)

        # Test the scoring engine using the generated pipeline that was deserialized
        inputColumns = pipeline.getPipeline()[2].getModelInputColumns()
        value = [ random() for _ in range(len(inputColumns))]

        fieldList = traininglauncher.getParser().fields       
        typeDict = {}
        for field in fieldList:
            typeDict[field['columnName']] = field['sqlType']

        lines = self.__getLineToScore(inputColumns, typeDict, value)
        rowDict1 = se.getRowToScore(lines[0])[1]
        resultFrame1 = se.predict(pipeline, rowDict1)

        rowDict2 = se.getRowToScore(lines[1])[1]
        resultFrame2 = se.predict(pipeline, rowDict2)
        print(lines[0])
        print(lines[1])
        print("Score = " + str(resultFrame1['Score'][0]))
        self.assertEquals(resultFrame1['Score'][0], resultFrame2['Score'][0])
        print("===========================================")
        # Generate the csv files
        testcase = 2
        values = []
        values.append(value)
        for i in range(testcase-1):
            values.append([random() for _ in range(len(inputColumns))])

        scores = self.__getPredictScore(pipeline, typeDict, values) 
        for i in range(len(scores)):
            print str(i+1)+", "+str(scores[i])
        self.__createCSV(inputColumns, values)


    def __getLineToScore(self, inputColumns, typeDict, value):
        columnWithValue = zip(inputColumns, value)
        line1 = self.__getLine(columnWithValue, typeDict)

        shuffle(columnWithValue)
        line2 = self.__getLine(columnWithValue, typeDict)

        return (line1, line2)

    def __getLine(self, columnsWithValue, typeDict):
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
        line = '{"key":"%s","value":%s}' % (str(uuid.uuid4()),line)
        return line

    def __createCSV(self, inputColumns, values):
        with open('./results/test.csv', 'wb') as csvfile:
            csvWriter = csv.writer(csvfile)
            csvWriter.writerow(['id']+inputColumns)
            for i in range(len(values)):
                csvWriter.writerow([i+1]+values[i])

    def __getPredictScore(self, pipeline, typeDict, values):
        scores = []
        inputColumns = pipeline.getPipeline()[2].getModelInputColumns()
        for value in values:
            row = self.__getLine(zip(inputColumns, value), typeDict)
            rowDict = se.getRowToScore(row)[1]
            resultFrame = se.predict(pipeline, rowDict)
            scores.append(resultFrame['Score'][0])
        return scores

    def __decodeBase64ThenDecompressToFile(self, data, filename):
        gzipByteArray = bytearray(base64.decodestring(data))
        with open(filename, "wb") as output:
            output.write(gzipByteArray)
        output.close()

        with gzip.GzipFile(filename, "rb") as compressed:
            data = compressed.read()
            with open(filename + ".decompressed", "wb") as decompressed:
                decompressed.write(data)
        compressed.close()
        decompressed.close()

        return decompressed.name