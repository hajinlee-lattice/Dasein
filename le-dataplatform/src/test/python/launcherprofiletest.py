import os
import shutil
from testbase import TestBase
from launcher import Launcher
from leframework.executors.learningexecutor import LearningExecutor

class DataProfileTest(TestBase):

    def setUp(self):
        script = "data_profile.py"
        if os.path.exists(script):
            os.remove(script)

        # Symbolic links will be cleaned up by testBase
        os.symlink("../../main/python/algorithm/" + script, script)
        results = "./results"
        if os.path.exists(results):
            shutil.rmtree(results)

    def testExecuteLearningForProfile(self):
        # These properties won't really be used since these are just unit tests.
        # Functional and end-to-end tests should be done from java
        os.environ["CONTAINER_ID"] = "xyz"
        os.environ["SHDP_HD_FSWEB"] = "localhost:50070"
        launcher = Launcher("model-dataprofile.json")
        launcher.execute(False)
        learningExecutor = LearningExecutor()

        results = learningExecutor.retrieveMetadata("./results/profile.avro", False)
        self.assertTrue(results is not None)

