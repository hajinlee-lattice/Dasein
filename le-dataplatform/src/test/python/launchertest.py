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


class LauncherTest(TestCase):

    @classmethod
    def setUpClass(cls):
        results = "./results"
        if os.path.exists(results):
            shutil.rmtree(results)
            
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

    def testExecute(self):
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
        self.assertTrue(isinstance(pipeline.getPipeline()[1].getModel(), RandomForestClassifier), "clf not instance of sklearn RandomForestClassifier.")
        
        pipelineScript = "./results/pipeline.py"
        self.__writeToFileFromBinary(jsonDict["Model"]["SupportFiles"][1]["Value"], pipelineScript)
        self.assertTrue(filecmp.cmp(pipelineScript, './leframework.tar.gz/pipeline.py'))
        
        encoderScript = "./results/encoder.py"
        self.__writeToFileFromBinary(jsonDict["Model"]["SupportFiles"][0]["Value"], encoderScript)
        self.assertTrue(filecmp.cmp(encoderScript, './leframework.tar.gz/encoder.py'))

        self.assertTrue(jsonDict["Model"]["Script"] is not None)
        
        # Test the scoring engine using the generated pipeline that was deserialized
        lines = self.__getLineToScore(pipeline)
        rowDict1 = se.getRowToScore(lines[0])
        resultFrame1 = se.predict(pipeline, rowDict1)
        
        rowDict2 = se.getRowToScore(lines[1])
        resultFrame2 = se.predict(pipeline, rowDict2)
        print(lines[0])
        print(lines[1])
        print("Score = " + str(resultFrame1['Score'][0]))
        self.assertEquals(resultFrame1['Score'][0], resultFrame2['Score'][0])
    
    def __getLineToScore(self, pipeline):
        '''
         This tests whether or not the order of input matters. It shuffles the columns so that
         to simulate VisiDB passing in data in a different order.
        '''
        inputColumns1 = pipeline.getPipeline()[1].getModelInputColumns()
        inputColumns2 = list(pipeline.getPipeline()[1].getModelInputColumns())
        shuffle(inputColumns2)
        valueMap = dict()
        line1 = self.__getLine(inputColumns1, valueMap, True)
        line2 = self.__getLine(inputColumns2, valueMap, False)
        return (line1, line2)
    
    def __getLine(self, inputColumns, valueMap, generateValue):
        line = "["
        first = True
        for inputColumn in inputColumns:
            if first:
                first = False
            else:
                line += ","
            
            if generateValue:
                value = random()
            else:
                value = valueMap[inputColumn]
            line += "{\"Key\":\"%s\",\"Value\":{\"SerializedValueAndType\":\"Float|'%s'\"}}" % (inputColumn, value)
            
            valueMap[inputColumn] = value
        line += "]"
        return line
    
    def __writeToFileFromBinary(self, data, filename):
        pklByteArray = bytearray(data)
        # Write to the file system
        
        with open(filename, "wb") as output:
            output.write(pklByteArray)
        
