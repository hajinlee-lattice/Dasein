import os
import sys

from trainingtestbase import TrainingTestBase

class EnhancedFilesTest(TrainingTestBase):

    def tearDown(self):
        super(TrainingTestBase, self).tearDown()
        self.tearDownClass()
        self.setUpClass()

    def testEnhancedSummary(self):
        self.launch("model.json")
        self.check_model_summary()
        self.check_data_composition()
        self.check_score_derivation()

    def launch(self, model):
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules: del sys.modules['launcher']
        from launcher import Launcher
        launcher = Launcher(model)
        launcher.execute(False)
        launcher.training

    def check_model_summary(self):
        # Output File Exists?
        outputFile = "./results/enhancements/modelsummary.json"
        self.assertTrue(os.path.isfile(outputFile))

    def check_data_composition(self):
        # Output File Exists?        
        outputFile = "./results/enhancements/DataComposition.json"
        self.assertTrue(os.path.isfile(outputFile))

    def check_score_derivation(self):
        # Output File Exists?       
        outputFile = "./results/enhancements/ScoreDerivation.json"
        self.assertTrue(os.path.isfile(outputFile))
