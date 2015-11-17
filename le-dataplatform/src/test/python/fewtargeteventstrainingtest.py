import json
import sys
from trainingtestbase import TrainingTestBase
import glob

class FewTargetEventsTrainingTest(TrainingTestBase):

    def testExecuteLearning(self):
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher
        
        traininglauncher = Launcher("model-dp410.json")
        traininglauncher.execute(False)
        # Retrieve the pickled model from the json file
        jsonDict = json.loads(open(glob.glob("./results/*.json")[0]).read())
        rocScore = jsonDict["Summary"]["RocScore"]
        print("Roc score = %f" % rocScore)
        
        self.assertTrue(jsonDict["Model"]["Script"] is not None)
        self.assertTrue(jsonDict["NormalizationBuckets"] is not None)
        self.assertTrue(len(jsonDict["NormalizationBuckets"]) > 0)
        
        self.assertFalse(rocScore == 0)

