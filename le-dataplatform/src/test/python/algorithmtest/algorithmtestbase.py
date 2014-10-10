import json
import os
import shutil
import sys
 
from testbase import TestBase

class AlgorithmTestBase(TestBase):

    def setUp(self):
        if os.path.exists("./results"):
            shutil.rmtree("./results")
        
    def tearDown(self):
        os.remove(self.algorithmFileName)
        os.remove(self.algorithmJsonFileName)
        
            
    def execute(self, algorithmFileName, algorithmProperties):
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher

        model = json.loads(open("model.json").read())
        model["algorithm_properties"] = algorithmProperties
        model["python_script"] = algorithmFileName
        algorithmJsonFileName = algorithmFileName + ".json"
        with open(algorithmJsonFileName, "w") as outfile:
            json.dump(model, outfile)
        os.environ["CONTAINER_ID"] = "xyz"
        os.environ["SHDP_HD_FSWEB"] = "localhost:50070"
        shutil.copyfile("../../main/python/algorithm/" + algorithmFileName, algorithmFileName)
        
        self.algorithmFileName = algorithmFileName
        self.algorithmJsonFileName = algorithmJsonFileName 
        launcher = Launcher(algorithmJsonFileName)
        launcher.execute(False)
        return launcher.getClassifier()
