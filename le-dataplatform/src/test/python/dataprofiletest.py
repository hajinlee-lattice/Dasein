import os
import shutil
from testbase import TestBase
from launcher import Launcher
from leframework.executors.learningexecutor import LearningExecutor

class DataProfileTest(TestBase):

    def setUp(self):
        # Simulate what happens in yarn when it copies the framework code over
        # before running the python script
        fwkdir = "./leframework.tar.gz"
        if os.path.exists(fwkdir):
            shutil.rmtree(fwkdir)
        if os.path.exists("data_profile.py"):
            os.remove("data_profile.py")

        os.makedirs(fwkdir + "/leframework")
        enginedir = "/leframework/scoringengine.py"
        shutil.copyfile("../../main/python" + enginedir, fwkdir + enginedir)
        shutil.copyfile("../../main/python/pipeline.py", fwkdir + "/pipeline.py")
        shutil.copyfile("../../main/python/encoder.py", fwkdir + "/encoder.py")
        shutil.copyfile("../../main/python/algorithm/lr_train.py", "./lr_train.py")
        shutil.copyfile("../../main/python/algorithm/rf_train.py", "./rf_train.py")
        shutil.copyfile("../../main/python/algorithm/data_profile.py", "./data_profile.py")
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

