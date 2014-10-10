import json
import os
import shutil
import sys
from testbase import TestBase


class TrainingTest(TestBase):

    def setUp(self):
        # Simulate what happens in yarn when it copies the framework code over
        # before running the python script
        self.fwkdir = "./leframework.tar.gz"
        self.pipelinefwkdir = "./lepipeline.tar.gz"
        fwkdir = self.fwkdir
        pipelinefwkdir = self.pipelinefwkdir

        if os.path.exists(fwkdir):
            shutil.rmtree(fwkdir)
        if os.path.exists(pipelinefwkdir):
            shutil.rmtree(pipelinefwkdir)

        os.makedirs(fwkdir + "/leframework")
        os.makedirs(pipelinefwkdir)

        enginedir = "/leframework/scoringengine.py"
        shutil.copyfile("../../main/python" + enginedir, fwkdir + enginedir)
        shutil.copyfile("../../main/python/pipeline/pipeline.py", "./pipeline.py")
        shutil.copyfile("../../main/python/pipeline/encoder.py", pipelinefwkdir + "/encoder.py")
        sys.path.append(pipelinefwkdir)

        # Symbolic links will be cleaned up by testBase
        scriptDir = "../../main/python/algorithm/" 
        for f in os.listdir(scriptDir):
            fPath = os.path.join(scriptDir, f)
            if os.path.isfile(fPath) and not os.path.exists(f):
                os.symlink(fPath, f)

        results = "./results"
        if os.path.exists(results):
            shutil.rmtree(results)

    def testExecuteLearning(self):
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher
        
        # These properties won't really be used since these are just unit tests.
        # Functional and end-to-end tests should be done from java
        os.environ["CONTAINER_ID"] = "xyz"
        os.environ["SHDP_HD_FSWEB"] = "localhost:50070"
        traininglauncher = Launcher("model-dp410.json")
        traininglauncher.execute(False)
        # Retrieve the pickled model from the json file
        jsonDict = json.loads(open("./results/model.json").read())
        rocScore = jsonDict["Summary"]["RocScore"]
        print("Roc score = %f" % rocScore)
        self.assertFalse(rocScore == 0)

