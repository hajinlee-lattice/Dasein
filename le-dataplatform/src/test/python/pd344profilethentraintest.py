import glob
import json
import os
import sys

from leframework.executors.learningexecutor import LearningExecutor
from trainingtestbase import TrainingTestBase


class PD344ProfilingThenTrainTest(TrainingTestBase):

    def testExecuteProfilingThenTrain(self):
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher

        # These properties won't really be used since these are just unit tests.
        # Functional and end-to-end tests should be done from java
        profilinglauncher = Launcher("profiledriver-pd344.json")
        profilinglauncher.execute(False)
        learningExecutor = LearningExecutor()

        results = learningExecutor.retrieveMetadata("./results/profile.avro", False)
        self.assertTrue(results is not None)

        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher
        
        os.symlink("./results/profile.avro", "profile-pd344.avro")

        traininglauncher = Launcher("modeldriver-pd344.json")
        traininglauncher.execute(False)
        
        jsonDict = json.loads(open(glob.glob("./results/*.json")[0]).read())
        
        


    def tearDown(self):
        # Remove launcher module to restore its globals()
        del sys.modules['launcher']

