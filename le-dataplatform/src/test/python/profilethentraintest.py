import glob
import json
import os
import sys

from leframework.executors.learningexecutor import LearningExecutor
from trainingtestbase import TrainingTestBase


class ProfilingThenTrainTest(TrainingTestBase):


    def testExecuteProfilingThenTrain(self):
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher

        # These properties won't really be used since these are just unit tests.
        # Functional and end-to-end tests should be done from java
        profilinglauncher = Launcher("model-badlift-profiling.json")
        profilinglauncher.parser.highUCThreshold = 0.000001
        
        profilinglauncher.execute(False, postProcessClf=False)
        learningExecutor = LearningExecutor()

        results = learningExecutor.retrieveMetadata("./results/profile.avro", False)
        self.assertTrue(results is not None)
        
        self.assertProfilingData()

        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher

        os.symlink("./results/profile.avro", "profile-badlift.avro")
        traininglauncher = Launcher("model-badlift-training.json")
        traininglauncher.execute(False)
        
        jsonDict = json.loads(open(glob.glob("./results/*.json")[0]).read())

    def assertProfilingData(self):
        diagnosticsJsonDict = json.loads(open("./results/diagnostics.json").read())
        self.assertEqual(diagnosticsJsonDict["Summary"]["NumberOfSkippedRows"], 0)
        self.assertTrue(len(diagnosticsJsonDict["Summary"]["HighUCColumns"]) > 0)
        
    def tearDown(self):
        # Remove launcher module to restore its globals()
        del sys.modules['launcher']

