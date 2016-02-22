import csv
import glob
import json
import os
import pickle
from sklearn.ensemble import RandomForestClassifier
import subprocess
import sys
import shutil
from distutils.dir_util import copy_tree

from testbase import removeFiles
from trainingtestbase import TrainingTestBase


class EVPipelineTrainingTest(TrainingTestBase):
    
    def setUp(self):
        super(EVPipelineTrainingTest, self).setUp()
        shutil.rmtree("./evpipeline.tar.gz", ignore_errors=True)
        os.makedirs("./evpipeline.tar.gz")
        shutil.copy("../../main/python/evpipeline/evpipelinesteps.py", "./evpipeline.tar.gz/evpipelinesteps.py")
        shutil.copy("../../main/python/pipeline/encoder.py", "./evpipeline.tar.gz/encoder.py")
        os.symlink("../../main/python/evpipeline/evpipeline.py", "evpipeline.py")
        sys.path.append("./evpipeline.tar.gz")
        copy_tree("../../main/python/configurablepipelinetransformsfromfile", "./evpipeline.tar.gz/")
    
    def tearDown(self):
        super(EVPipelineTrainingTest, self).tearDown()
        shutil.rmtree("./evpipeline.tar.gz", ignore_errors=True)

    def testExecuteLearning(self):
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from evpipelinesteps import EVModelStep
        from launcher import Launcher

        
        traininglauncher = Launcher("modeldriver-evpipeline.json")
        traininglauncher.execute(False)

        # Retrieve the pickled model from the json file
        jsonDict = json.loads(open(glob.glob("./results/*.json")[0]).read())

        for index in range(0, len(jsonDict["Model"]["CompressedSupportFiles"])):
            entry = jsonDict["Model"]["CompressedSupportFiles"][index]
            fileName = "./results/" + entry["Key"] + ".gz"
            self.decodeBase64ThenDecompressToFile(entry["Value"], fileName)
            if entry["Key"].find('STPipelineBinary') >= 0:
                pipeline = pickle.load(open(fileName, "r"))
                self.assertTrue(isinstance(pipeline.getPipeline()[4].getModel(), RandomForestClassifier), "clf not instance of sklearn RandomForestClassifier.")
                self.assertTrue(isinstance(pipeline.getPipeline()[4], EVModelStep), "No post score step.")
            os.rename(fileName, "./results/" + entry["Key"])

        self.createCSVFromModel("modeldriver-evpipeline.json", "./results/scoreinputfile.txt")
        
        with open("./results/scoringengine.py", "w") as scoringScript:
            scoringScript.write(jsonDict["Model"]["Script"])

#         os.environ["PYTHONPATH"] = ''
        popen = subprocess.Popen([sys.executable, "./results/scoringengine.py", "./results/scoreinputfile.txt", "./results/scoreoutputfile.txt"], \
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        _, stderr = popen.communicate()
        self.assertEquals(len(stderr), 0, str(stderr))
        
        tokens = csv.reader(open("./results/scoreoutputfile.txt", "r")).next()
        self.assertEquals(len(tokens), 3)

