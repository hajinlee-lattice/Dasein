import filecmp
import json
import os
import pickle
from random import random
import sys
import glob
from sklearn.ensemble import RandomForestClassifier

from leframework import scoringengine as se
from trainingtestbase import TrainingTestBase


class TrainingTest(TrainingTestBase):

    def testExecuteLearning(self):
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher
        
        traininglauncher = Launcher("model.json")
        traininglauncher.execute(False)
        t = traininglauncher.training;
        
        # Retrieve the pickled model from the json file
        jsonDict = json.loads(open(glob.glob("./results/*.json")[0]).read())

        pipelineScript = "./results/pipeline.py.gz"
        self.decodeBase64ThenDecompressToFile(jsonDict["Model"]["CompressedSupportFiles"][0]["Value"], pipelineScript)
        self.assertTrue(filecmp.cmp(pipelineScript + ".decompressed", './pipeline.py'))

        payload = "./results/STPipelineBinary.p.gz"
        self.decodeBase64ThenDecompressToFile(jsonDict["Model"]["CompressedSupportFiles"][1]["Value"], payload)
        # Load from the file system and deserialize into the model
        pipeline = pickle.load(open(payload + ".decompressed", "r"))
        self.assertTrue(isinstance(pipeline.getPipeline()[2].getModel(), RandomForestClassifier), 
                        "clf not instance of sklearn RandomForestClassifier.")

        pipelineFwk = "./results/pipelinefwk.py.gz"
        self.decodeBase64ThenDecompressToFile(jsonDict["Model"]["CompressedSupportFiles"][2]["Value"], pipelineFwk)
        self.assertTrue(filecmp.cmp(pipelineFwk + ".decompressed", './pipelinefwk.py'))

        encoderScript = "./results/encoder.py.gz"
        self.decodeBase64ThenDecompressToFile(jsonDict["Model"]["CompressedSupportFiles"][3]["Value"], encoderScript)
        self.assertTrue(filecmp.cmp(encoderScript + ".decompressed", './lepipeline.tar.gz/encoder.py'))

        pipelineStepsScript = "./results/pipelinesteps.py.gz"
        self.decodeBase64ThenDecompressToFile(jsonDict["Model"]["CompressedSupportFiles"][4]["Value"], pipelineStepsScript)
        self.assertTrue(filecmp.cmp(pipelineStepsScript + ".decompressed", './lepipeline.tar.gz/pipelinesteps.py'))

        self.assertTrue(jsonDict["Model"]["Script"] is not None)

        # Test the scoring engine using the generated pipeline that was deserialized
        inputColumns = pipeline.getPipeline()[2].getModelInputColumns()
        value = [ random() for _ in range(len(inputColumns))]

        fieldList = traininglauncher.getParser().fields
        typeDict = {}
        for field in fieldList:
            typeDict[field['columnName']] = field['sqlType']
          
        lines = self.getLineToScore(inputColumns, typeDict, value)
        rowDicts = []
        rowDicts.append(se.getRowToScore(lines[0])[1])
        resultFrame1 = se.predict(pipeline, rowDicts)
        rowDicts = []
        rowDicts.append(se.getRowToScore(lines[1])[1])
        resultFrame2 = se.predict(pipeline, rowDicts)
        print(lines[0])
        print(lines[1])
        print("Score = " + str(resultFrame1['Score'][0]))
        self.assertEquals(resultFrame1['Score'][0], resultFrame2['Score'][0])
        print("===========================================")
        # Generate the csv files
        testcase = 2
        values = []
        values.append(value)
        for i in range(testcase - 1):
            values.append([random() for _ in range(len(inputColumns))])

        scores = self.getPredictScore(pipeline, typeDict, values) 
        for i in range(len(scores)):
            print str(i + 1) + ", " + str(scores[i])
        self.createCSV(inputColumns, values)
        
        #self.__generateScoringInput(pipeline, t, inputColumns, typeDict)
       
    def generateScoringInput(self, pipeline, t, inputColumns, typeDict):
        lines = []
        w = open("scoringtestinput.txt", 'wb')
        i = 0;
        values = []
        dataValues = t.as_matrix(inputColumns)
        print "size of the inputColumns: " + str(len(inputColumns))
        print "length of t0 " + str(len(dataValues[0]))
        for data in dataValues:
            if len(data) != 100:
                print "not equal to 100 " + str(data)
            if i >= 100:
                break;
            line = self.getLineToScore2(inputColumns, typeDict, data)
            values.append(data)
            w.write(line + "\n")
            lines.append(line)
            i = i + 1
        print i
        w.close()
        rowDicts = []
        for line in lines:
            rowDicts.append(se.getRowToScore(line)[1])
        se.predict(pipeline, rowDicts)
        self.createCSV(inputColumns, values)
        
