import csv
import glob
import json
import os
import pickle
import shutil
import subprocess
import sys

from trainingtestbase import TrainingTestBase


class ParallelEVPipelineRegressionTrainingTest(TrainingTestBase):
    
    def setUp(self):
        super(ParallelEVPipelineRegressionTrainingTest, self).setUp()
    
    def tearDown(self):
        super(ParallelEVPipelineRegressionTrainingTest, self).tearDown()
        shutil.rmtree("./evpipeline.tar.gz", ignore_errors=True)

    def testExecuteLearning(self):
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from evpipelinesteps import EVModelStep
        from launcher import Launcher
        from aggregatedmodel import AggregatedModel
        
        traininglauncher = Launcher("modeldriver-regression-evpipeline.json")
        traininglauncher.execute(False)

        print("Modeling pipeline done.")
        os.unlink("model.p")
        os.symlink("./results/model.p", "model.p")
        
        traininglauncher = Launcher("modeldriver-regression-aggregation-evpipeline.json")
        traininglauncher.execute(False)
        print("Modeling aggregation pipeline done.")
        
        # Retrieve the pickled model from the json file
        jsonDict = json.loads(open(glob.glob("./results/*.json")[0]).read())

        for index in range(0, len(jsonDict["Model"]["CompressedSupportFiles"])):
            entry = jsonDict["Model"]["CompressedSupportFiles"][index]
            fileName = "./results/" + entry["Key"] + ".gz"
            self.decodeBase64ThenDecompressToFile(entry["Value"], fileName)
            if entry["Key"].find('STPipelineBinary') >= 0:
                pipeline = pickle.load(open(fileName, "r"))
                self.assertTrue(isinstance(pipeline.getPipeline()[6].getModel(), AggregatedModel), "clf not instance of AggregatedModel.")
                self.assertTrue(isinstance(pipeline.getPipeline()[7], EVModelStep), "No post score step.")
                self.assertIsNotNone(pipeline.getPipeline()[7].model)
                self.assertTrue(isinstance(pipeline.getPipeline()[7].model, AggregatedModel), "clf not instance of AggregatedModel.")
                self.assertTrue(len(pipeline.getPipeline()[7].model.models) == 1, "There no models found.")
                self.assertTrue(len(pipeline.getPipeline()[7].model.regressionModels) == 1, "There no regression models found.")
                
            os.rename(fileName, "./results/" + entry["Key"])

        self.createCSVFromModel("modeldriver-regression-aggregation-evpipeline.json", "./results/scoreinputfile.txt")
        
        print("CSV from model created.")
        with open("./results/scoringengine.py", "w") as scoringScript:
            scoringScript.write(jsonDict["Model"]["Script"])

        os.environ["PYTHONPATH"] = '/usr/local/lib/python2.7/site-packages:./evpipeline.tar.gz:./lepipeline.tar.gz'
        popen = subprocess.Popen([sys.executable, "./results/scoringengine.py", "./results/scoreinputfile.txt", "./results/scoreoutputfile.txt"], \
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print("Scoring done.")
        _, stderr = popen.communicate()
        print stderr
        tokens = csv.reader(open("./results/scoreoutputfile.txt", "r")).next()
        self.assertEquals(len(tokens), 3, "Length != 3")
