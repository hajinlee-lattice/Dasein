import os
import sys
import shutil
from testbase import TestBase
from leframework.executors.learningexecutor import LearningExecutor

class ProfilingTest(TestBase):

    def setUp(self):
        script = "data_profile.py"
        if os.path.exists(script):
            os.remove(script)

        # Symbolic links will be cleaned up by testBase
        os.symlink("../../main/python/algorithm/" + script, script)
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
        profilinglauncher = Launcher("bad-target-dataprofile.json")
        profilinglauncher.execute(False)
        learningExecutor = LearningExecutor()

        results = learningExecutor.retrieveMetadata("./results/profile.avro", False)
        self.assertTrue(results is not None)
    
    def tearDown(self):
        # Remove launcher module to restore its globals()
        del sys.modules['launcher'] 

