import os
import shutil
import sys

from leframework.executors.learningexecutor import LearningExecutor
from testbase import TestBase


class ProfilingThenTrainTest(TestBase):

    def setUp(self):
        profileScript = "data_profile.py"
        if os.path.exists(profileScript):
            os.remove(profileScript)
        trainingScript = "rf_train.py"
        if os.path.exists(trainingScript):
            os.remove(trainingScript)

        # Symbolic links will be cleaned up by testBase
        os.symlink("../../main/python/algorithm/" + profileScript, profileScript)
        os.symlink("../../main/python/algorithm/" + trainingScript, trainingScript)
        results = "./results"
        if os.path.exists(results):
            shutil.rmtree(results)

    def testExecuteProfiling(self):
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher

        # These properties won't really be used since these are just unit tests.
        # Functional and end-to-end tests should be done from java
        os.environ["CONTAINER_ID"] = "xyz"
        os.environ["SHDP_HD_FSWEB"] = "localhost:50070"
        profilinglauncher = Launcher("model-badlift-profiling.json")
        profilinglauncher.execute(False)
        learningExecutor = LearningExecutor()

        results = learningExecutor.retrieveMetadata("./results/profile.avro", False)
        self.assertTrue(results is not None)

        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher
        
        os.symlink("./results/profile.avro", "profile-badlift.avro")

        traininglauncher = Launcher("model-badlift-training.json")
        traininglauncher.execute(False)


    def tearDown(self):
        # Remove launcher module to restore its globals()
        del sys.modules['launcher']

