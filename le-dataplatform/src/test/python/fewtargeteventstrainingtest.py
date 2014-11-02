import json
import sys
from trainingtestbase import TrainingTestBase


class FewTargetEventsTrainingTest(TrainingTestBase):

    def testExecuteLearning(self):
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher
        
        traininglauncher = Launcher("model-dp410.json")
        traininglauncher.execute(False)
        # Retrieve the pickled model from the json file
        jsonDict = json.loads(open("./results/model.json").read())
        rocScore = jsonDict["Summary"]["RocScore"]
        print("Roc score = %f" % rocScore)
        self.assertFalse(rocScore == 0)

