import json
import os
import sys
 
from trainingtestbase import TrainingTestBase

class AlgorithmTestBase(TrainingTestBase):
    
    algorithmJsonFileName = ""

    def tearDown(self):
        if os.path.exists(self.algorithmJsonFileName):
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
        os.environ["CONTAINER_ID"] = "container_1425511391553_3644_01_000001"
        os.environ["SHDP_HD_FSWEB"] = "localhost:50070"

        self.algorithmJsonFileName = algorithmJsonFileName 
        launcher = Launcher(algorithmJsonFileName)
        launcher.execute(False)
        return launcher.getClassifier()
