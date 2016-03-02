import glob
import json
import sys

from trainingtestbase import TrainingTestBase

class CuratedTrainingTest(TrainingTestBase):

    def testExecuteLearning(self):
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher

        traininglauncher = Launcher("modeldriver-curated.json")
        traininglauncher.execute(False)

        jsonDict = json.loads(open(glob.glob("./results/*.json")[0]).read())
        self.assertTrue(jsonDict["Summary"]["RocScore"] > 0.0)
        self.assertTrue(jsonDict["Model"]["Script"] is not None)
