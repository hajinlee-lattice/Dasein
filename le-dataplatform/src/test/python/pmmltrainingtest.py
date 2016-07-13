from trainingtestbase import TrainingTestBase
 
import sys
import json
import glob
 
class PmmlTrainingTest(TrainingTestBase):
 
    def testExecuteLearning(self):
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher
 
        traininglauncher = Launcher("modeldriver-pmml.json")
        traininglauncher.execute(False)

        jsonDict = json.loads(open(glob.glob("./results/enhancements/modelsummary.json")[0]).read())
        self.assertEquals(jsonDict["ModelDetails"]["ModelType"], "PmmlModel")