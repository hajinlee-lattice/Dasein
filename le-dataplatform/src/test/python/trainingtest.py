import filecmp
import glob
import json
import os
import pickle
from random import random
from sklearn.ensemble import RandomForestClassifier
import sys

from leframework import scoringengine as se
from trainingtestbase import TrainingTestBase


class TrainingTest(TrainingTestBase):

    def testExecuteLearning(self):
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher

        traininglauncher = Launcher("model.json")
        fieldList = traininglauncher.getParser().fields
        traininglauncher.execute(False)
 
        # Retrieve the pickled model from the json file
        jsonDict = json.loads(open(glob.glob("./results/*.json")[0]).read())

        foundStandardFunction = False
        for index in range(0, len(jsonDict["Model"]["CompressedSupportFiles"])):
            entry = jsonDict["Model"]["CompressedSupportFiles"][index]
            fileName = "./results/" + entry["Key"] + ".gz"
            self.decodeBase64ThenDecompressToFile(entry["Value"], fileName)
            if entry["Key"].find('STPipelineBinary') >= 0:
                pipeline = pickle.load(open(fileName, "r"))
                print pipeline.getPipeline()
                self.assertTrue(isinstance(self.getModelStep(pipeline).getModel(), RandomForestClassifier), \
                                "clf not instance of sklearn RandomForestClassifier.")
            elif entry["Key"].find('encoder') >= 0 or \
                 entry["Key"].find('pipelinesteps') >= 0 or \
                 entry["Key"].find('aggregatedmodel') >= 0 or \
                 entry["Key"].find('make_float') >= 0 or \
                 entry["Key"].find('pipeline.py') >= 0 or \
                 entry["Key"].find('pipeline.json') >= 0 or \
                 entry["Key"].find('replace_null_value') >= 0:
                self.assertTrue(filecmp.cmp(fileName, './lepipeline.tar.gz/' + entry["Key"]))
            else:
                if os.path.exists('./' + entry["Key"]):
                    self.assertTrue(filecmp.cmp(fileName, './' + entry["Key"]))
                elif os.path.exists('./lepipeline.tar.gz/' + entry["Key"]):
                    self.assertTrue(filecmp.cmp(fileName, './lepipeline.tar.gz/' + entry["Key"]))
            if entry["Key"].startswith("std_"):
                foundStandardFunction = True

        self.assertTrue(foundStandardFunction)
        self.assertTrue(jsonDict["Model"]["Script"] is not None)
        self.assertTrue(jsonDict["NormalizationBuckets"] is not None)
        self.assertTrue(len(jsonDict["NormalizationBuckets"]) > 0)
        self.assertEquals(jsonDict["Model"]["LatticeVersion"], "2.0.22-SNAPSHOT")
        self.assertEquals(jsonDict["Model"]["RandomSeed"], "99999")

        # Test the scoring engine using the generated pipeline that was deserialized
        inputColumns = json.loads(open(glob.glob("model.json")[0]).read())["features"]

        value = [random() for _ in range(len(inputColumns))]

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
