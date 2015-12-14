import filecmp
import glob
import json
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
 
        for index in range(0, len(jsonDict["Model"]["CompressedSupportFiles"])):
            entry = jsonDict["Model"]["CompressedSupportFiles"][index]
            fileName = "./results/" + entry["Key"] + ".gz"
            self.decodeBase64ThenDecompressToFile(entry["Value"], fileName)
            if entry["Key"].find('STPipelineBinary') >= 0:
                pipeline = pickle.load(open(fileName, "r"))
                self.assertTrue(isinstance(pipeline.getPipeline()[3].getModel(), RandomForestClassifier), "clf not instance of sklearn RandomForestClassifier.")
            elif entry["Key"].find('encoder') >= 0 or entry["Key"].find('pipelinesteps') >= 0 or entry["Key"].find('aggregatedmodel') >= 0: self.assertTrue(filecmp.cmp(fileName, './lepipeline.tar.gz/' + entry["Key"]))
            else: self.assertTrue(filecmp.cmp(fileName, './' + entry["Key"]))
 
        self.assertTrue(jsonDict["Model"]["Script"] is not None)
        self.assertTrue(jsonDict["NormalizationBuckets"] is not None)
        self.assertTrue(len(jsonDict["NormalizationBuckets"]) > 0)
         
        # Test the scoring engine using the generated pipeline that was deserialized
        allInputColumns = pipeline.getPipeline()[3].getModelInputColumns()
        inputColumns = []
        for col in allInputColumns:
            if "Transformed_Boolean" not in col:
                inputColumns.append(col)
                 
        value = [ random() for _ in range(len(inputColumns))]
 
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
 
        # self.__generateScoringInput(pipeline, t, inputColumns, typeDict)
 
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