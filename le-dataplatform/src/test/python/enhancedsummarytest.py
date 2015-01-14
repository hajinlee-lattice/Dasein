import os
import sys

from trainingtestbase import TrainingTestBase

class EnhancedSummaryTest(TrainingTestBase):

    def tearDown(self):
        super(TrainingTestBase, self).tearDown()
        self.tearDownClass()
        self.setUpClass()

    def testEnhancedSummary(self):
        self.launch("model.json")
        self.checkResults()

    def launch(self, model):
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules: del sys.modules['launcher']
        from launcher import Launcher
        launcher = Launcher(model)
        launcher.execute(False)
        launcher.training

    def checkResults(self):
        # Output File Exists?
        outputFile = "./results/enhancements/modelsummary.json"
        self.assertTrue(os.path.isfile(outputFile))
