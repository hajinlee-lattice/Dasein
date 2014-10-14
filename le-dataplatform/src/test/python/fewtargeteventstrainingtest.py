import json
import os
import sys
from trainingtestbase import TrainingTestBase


class FewTargetEventsTrainingTest(TrainingTestBase):

    def testExecuteLearning(self):
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher
        
        # These properties won't really be used since these are just unit tests.
        # Functional and end-to-end tests should be done from java
        os.environ["CONTAINER_ID"] = "xyz"
        os.environ["SHDP_HD_FSWEB"] = "localhost:50070"
        traininglauncher = Launcher("model-dp410.json")
        traininglauncher.execute(False)
        # Retrieve the pickled model from the json file
        jsonDict = json.loads(open("./results/model.json").read())
        rocScore = jsonDict["Summary"]["RocScore"]
        print("Roc score = %f" % rocScore)
        self.assertFalse(rocScore == 0)

