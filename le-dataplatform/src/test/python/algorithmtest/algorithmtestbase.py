import json
import os
import shutil
import sys
 
from testbase import TestBase

class AlgorithmTestBase(TestBase):
    
    algorithmFileName = ""
    algorithmJsonFileName = ""

    def setUp(self):
        if os.path.exists("./results"):
            shutil.rmtree("./results")
                 
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
        shutil.copyfile("../../main/python/pipeline/pipelinesteps.py", pipelinefwkdir + "/pipelinesteps.py")
        
    def tearDown(self):
        if os.path.exists(self.fwkdir):
            shutil.rmtree(self.fwkdir)
        if os.path.exists(self.pipelinefwkdir):
            shutil.rmtree(self.pipelinefwkdir)
        if os.path.exists("./pipeline.py"):
            os.remove("./pipeline.py")
        os.remove(self.algorithmFileName)
        os.remove(self.algorithmJsonFileName)
        
            
    def execute(self, algorithmFileName, algorithmProperties):
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher
        
        model = json.loads(open("model.json").read())
        model["algorithm_properties"] = algorithmProperties
        model["python_script"] = algorithmFileName
        algorithmJsonFileName = algorithmFileName + ".json"
        with open(algorithmJsonFileName, "w") as outfile:
            json.dump(model, outfile)
        os.environ["CONTAINER_ID"] = "xyz"
        os.environ["SHDP_HD_FSWEB"] = "localhost:50070"
        shutil.copyfile("../../main/python/algorithm/" + algorithmFileName, algorithmFileName)
        
        
        self.algorithmFileName = algorithmFileName
        self.algorithmJsonFileName = algorithmJsonFileName 
        launcher = Launcher(algorithmJsonFileName)
        launcher.execute(False)
        return launcher.getClassifier()
