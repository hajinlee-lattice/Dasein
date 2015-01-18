import csv
import glob
import json
import os
import pickle
from sklearn.ensemble import RandomForestClassifier
import subprocess
import sys

from testbase import removeFiles
from trainingtestbase import TrainingTestBase


class EVPipelineTrainingTest(TrainingTestBase):
    
    def setUp(self):
        super(EVPipelineTrainingTest, self).setUp()
        os.symlink("./data/evpipeline.tar.gz", "./evpipeline.tar.gz")
        sys.path.append("./evpipeline.tar.gz")
    
    def tearDown(self):
        super(EVPipelineTrainingTest, self).tearDown()
        removeFiles("./evpipeline.tar.gz")

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
                self.assertTrue(isinstance(pipeline.getPipeline()[3].getModel(), RandomForestClassifier), "clf not instance of sklearn RandomForestClassifier.")
                self.assertTrue(isinstance(pipeline.getPipeline()[4], EVModelStep), "No post score step.")
                self.assertEquals(pipeline.getPipeline()[4].getProperty("provenanceProperties")["EVModelColumns"], 
                                  "__Revenue_0,__Revenue_1,__Revenue_2,__Revenue_3,__Revenue_4,__Revenue_5,__Revenue_6,__Revenue_7,__Revenue_8,__Revenue_9,__Revenue_10,__Revenue_11")
            os.rename(fileName, "./results/" + entry["Key"])

        self.createCSVFromModel("modeldriver-evpipeline.json", "./results/scoreinputfile.txt")
        
        with open("./results/scoringengine.py", "w") as scoringScript:
            scoringScript.write(jsonDict["Model"]["Script"])

        os.environ["PYTHONPATH"] = ''
        popen = subprocess.Popen([sys.executable, "./results/scoringengine.py", "./results/scoreinputfile.txt", "./results/scoreoutputfile.txt"], \
                         stdout = subprocess.PIPE, stderr=subprocess.PIPE)
        _, stderr = popen.communicate()
        self.assertEquals(len(stderr), 0)
        
        tokens = csv.reader(open("./results/scoreoutputfile.txt", "r")).next()
        self.assertEquals(len(tokens), 3)

