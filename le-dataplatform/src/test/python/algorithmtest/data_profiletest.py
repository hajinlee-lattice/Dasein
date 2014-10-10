import json
import os

from algorithmtestbase import AlgorithmTestBase


class DataProfileTest(AlgorithmTestBase):

    def testTrain(self):
        clf = self.execute("data_profile.py", "")
        self.assertTrue(clf is None)
        self.assertTrue(os.path.exists("./results/profile.avro"))
        self.assertTrue(os.path.exists("./results/diagnostics.json"))
        try:
            diagnosticsJson = open("./results/diagnostics.json").read()
            _ = json.loads(diagnosticsJson)
        except:
            raise
