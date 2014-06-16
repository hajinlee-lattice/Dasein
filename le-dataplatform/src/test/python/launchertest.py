import csv
import filecmp
import json
import os
import pickle
from random import random
from random import shuffle
import shutil
from sklearn.ensemble import RandomForestClassifier
from unittest import TestCase

from launcher import Launcher
from leframework import scoringengine as se
from leframework.model.statemachine import StateMachine
from leframework.model.states.initialize import Initialize


class LauncherTest(TestCase):

    @classmethod
    def setUpClass(cls):
            
        # Simulate what happens in yarn when it copies the framework code over
        # before running the python script
        fwkdir = "./leframework.tar.gz"
        if os.path.exists(fwkdir):
            shutil.rmtree(fwkdir)
        if os.path.exists("feature_selection.py"):
            os.remove("feature_selection.py")
            
        os.makedirs(fwkdir + "/leframework")
        enginedir = "/leframework/scoringengine.py"
        shutil.copyfile("../../main/python" + enginedir, fwkdir + enginedir)
        shutil.copyfile("../../main/python/pipeline.py", fwkdir + "/pipeline.py")
        shutil.copyfile("../../main/python/encoder.py", fwkdir + "/encoder.py")
        shutil.copyfile("../../main/python/algorithm/lr_train.py", "./lr_train.py")
        shutil.copyfile("../../main/python/algorithm/rf_train.py", "./rf_train.py")
        shutil.copyfile("../../main/python/algorithm/feature_selection.py", "./feature_selection.py")
    
    def setUp(self):
        results = "./results"
        if os.path.exists(results):
            shutil.rmtree(results)

    def testExecuteLearning(self):
        # These properties won't really be used since these are just unit tests.
        # Functional and end-to-end tests should be done from java
        os.environ["CONTAINER_ID"] = "xyz"
        os.environ["SHDP_HD_FSWEB"] = "localhost:50070"
        launcher = Launcher("model.json")
        launcher.execute(False)
        
        # Retrieve the pickled model from the json file
        jsonDict = json.loads(open("./results/model.json").read())
        
        payload = "./results/STPipelineBinary.p"
        self.__writeToFileFromBinary(jsonDict["Model"]["SupportFiles"][2]["Value"], payload)
        # Load from the file system and deserialize into the model
        pipeline = pickle.load(open(payload, "r"))
        self.assertTrue(isinstance(pipeline.getPipeline()[2].getModel(), RandomForestClassifier), "clf not instance of sklearn RandomForestClassifier.")
        
        pipelineScript = "./results/pipeline.py"
        self.__writeToFileFromBinary(jsonDict["Model"]["SupportFiles"][1]["Value"], pipelineScript)
        self.assertTrue(filecmp.cmp(pipelineScript, './leframework.tar.gz/pipeline.py'))
        
        encoderScript = "./results/encoder.py"
        self.__writeToFileFromBinary(jsonDict["Model"]["SupportFiles"][0]["Value"], encoderScript)
        self.assertTrue(filecmp.cmp(encoderScript, './leframework.tar.gz/encoder.py'))

        self.assertTrue(jsonDict["Model"]["Script"] is not None)
        
        # Test the scoring engine using the generated pipeline that was deserialized
        inputColumns = pipeline.getPipeline()[2].getModelInputColumns()
        value = [ random() for j in range(len(inputColumns))]
        lines = self.__getLineToScore(inputColumns, value)
        rowDict1 = se.getRowToScore(lines[0])
        resultFrame1 = se.predict(pipeline, rowDict1)
        
        rowDict2 = se.getRowToScore(lines[1])
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
            values.append([random() for j in range(len(inputColumns))])
        
        scores = self.__getPredictScore(pipeline, values) 
        for i in range(len(scores)):
            print str(i+1)+", "+str(scores[i])
        self.__createCSV(inputColumns, values)
        
            
    def __getLineToScore(self, inputColumns, value):
        columnWithValue = zip(inputColumns, value)
        line1 = self.__getLine(columnWithValue)
        
        shuffle(columnWithValue)
        line2 = self.__getLine(columnWithValue)
        
        return (line1, line2)
    
    def __getLine(self, columnsWithValue):
        line = "["
        first = True
        for i in range(len(columnsWithValue)):
            if first:
                first = False
            else:
                line += ","
            
            line += "{\"Key\":\"%s\",\"Value\":{\"SerializedValueAndType\":\"Float|'%s'\"}}" % (columnsWithValue[i][0], columnsWithValue[i][1])

        line += "]"
        return line
    
    def __createCSV(self,inputColumns, values):
        with open('./results/test.csv', 'wb') as csvfile:
            csvWriter = csv.writer(csvfile)
            csvWriter.writerow(['id']+inputColumns)
            for i in range(len(values)):
                csvWriter.writerow([i+1]+values[i])
        
    def __getPredictScore(self,pipeline, values):
        scores = []
        inputColumns = pipeline.getPipeline()[2].getModelInputColumns()
        for value in values:
            row = self.__getLine(zip(inputColumns, value))
            rowDict = se.getRowToScore(row)
            resultFrame = se.predict(pipeline, rowDict)
            scores.append(resultFrame['Score'][0])
        return scores
        
        
    def __writeToFileFromBinary(self, data, filename):
        pklByteArray = bytearray(data)
        # Write to the file system
        
        with open(filename, "wb") as output:
            output.write(pklByteArray)

    def testExecuteLearningForProfile(self):
        # These properties won't really be used since these are just unit tests.
        # Functional and end-to-end tests should be done from java
        os.environ["CONTAINER_ID"] = "xyz"
        os.environ["SHDP_HD_FSWEB"] = "localhost:50070"
        launcher = Launcher("model-dataprofile.json")
        launcher.execute(False)
        initialize = Initialize()
        stateMachine = StateMachine()
        stateMachine.addState(initialize, 1)
        mediator = stateMachine.getMediator()
        mediator.schema = dict()
        mediator.schema["metadata"] = "./results/metadata.avro"
        mediator.depivoted = False
          
        results = initialize.retrieveMetadata(mediator)
        self.assertTrue(results is not None)
