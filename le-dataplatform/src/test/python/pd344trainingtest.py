import filecmp
import glob
import json
import pickle
from sklearn.ensemble import RandomForestClassifier
import sys

from trainingtestbase import TrainingTestBase


class PD344TrainingTest(TrainingTestBase):

    def testExecuteLearning(self):
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher

        traininglauncher = Launcher("modeldriver-pd344-1.json")
        traininglauncher.execute(False)

        # Retrieve the pickled model from the json file
        jsonDict = json.loads(open(glob.glob("./results/*.json")[0]).read())
        self.assertTrue(jsonDict["NormalizationBuckets"] is not None)
        self.assertTrue(len(jsonDict["NormalizationBuckets"]) > 0)
        
        for index in range(0, len(jsonDict["Model"]["CompressedSupportFiles"])):
            entry = jsonDict["Model"]["CompressedSupportFiles"][index]
            fileName = "./results/" + entry["Key"] + ".gz"
            self.decodeBase64ThenDecompressToFile(entry["Value"], fileName)
            if entry["Key"].find('STPipelineBinary') >= 0:
                pipeline = pickle.load(open(fileName, "r"))
                self.assertTrue(isinstance(pipeline.getPipeline()[3].getModel(), RandomForestClassifier), "clf not instance of sklearn RandomForestClassifier.")
            elif entry["Key"].find('encoder') >= 0 or \
                 entry["Key"].find('pipelinesteps') >= 0 or \
                 entry["Key"].find('aggregatedmodel') >= 0 or \
                 entry["Key"].find('make_float') >= 0 or \
                 entry["Key"].find('replace_null_value') >= 0:
                self.assertTrue(filecmp.cmp(fileName, './lepipeline.tar.gz/' + entry["Key"]))
            else: 
                self.assertTrue(filecmp.cmp(fileName, './' + entry["Key"]))
