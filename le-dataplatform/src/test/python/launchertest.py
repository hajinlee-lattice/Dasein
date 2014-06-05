import filecmp
import json
import os
import pickle
import shutil
from sklearn.linear_model import LogisticRegression
from unittest import TestCase

from launcher import Launcher


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
        self.__writeToFileFromBinary(jsonDict["Model"]["SupportFiles"][1]["Value"], payload)
        # Load from the file system and deserialize into the model
        pipeline = pickle.load(open(payload, "r"))
        self.assertTrue(isinstance(pipeline.getPipeline()[0].getModel(), LogisticRegression), "clf not instance of sklearn LogisticRegression.")
        
        pipelineScript = "./results/pipeline.py"
        self.__writeToFileFromBinary(jsonDict["Model"]["SupportFiles"][0]["Value"], pipelineScript)
        self.assertTrue(filecmp.cmp(pipelineScript, './leframework.tar.gz/pipeline.py'))
        
        self.assertTrue(jsonDict["Model"]["Script"] is not None)
        
    def __writeToFileFromBinary(self, data, filename):
        pklByteArray = bytearray(data)
        # Write to the file system
        
        with open(filename, "wb") as output:
            output.write(pklByteArray)
        
