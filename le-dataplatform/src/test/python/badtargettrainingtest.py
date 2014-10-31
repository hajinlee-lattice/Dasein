import filecmp
import json
import os
import pickle
from sklearn.ensemble import RandomForestClassifier
import sys 

from trainingtestbase import TrainingTestBase


class BadTargetTrainingTest(TrainingTestBase):

    def testExecuteLearning(self):
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher

        # These properties won't really be used since these are just unit tests.
        # Functional and end-to-end tests should be done from java
        os.environ["CONTAINER_ID"] = "xyz"
        os.environ["SHDP_HD_FSWEB"] = "localhost:50070"
        traininglauncher = Launcher("bad-target.json")
        traininglauncher.execute(False)

        # Retrieve the pickled model from the json file
        jsonDict = json.loads(open("./results/model.json").read())

        pipelineScript = "./results/pipeline.py.gz"
        self.decodeBase64ThenDecompressToFile(jsonDict["Model"]["CompressedSupportFiles"][0]["Value"], pipelineScript)
        self.assertTrue(filecmp.cmp(pipelineScript + ".decompressed", './pipeline.py'))

        payload = "./results/STPipelineBinary.p.gz"
        self.decodeBase64ThenDecompressToFile(jsonDict["Model"]["CompressedSupportFiles"][1]["Value"], payload)
        # Load from the file system and deserialize into the model
        pipeline = pickle.load(open(payload + ".decompressed", "r"))
        self.assertTrue(isinstance(pipeline.getPipeline()[2].getModel(), RandomForestClassifier), "clf not instance of sklearn RandomForestClassifier.")

        pipelineFwk = "./results/pipelinefwk.py.gz"
        self.decodeBase64ThenDecompressToFile(jsonDict["Model"]["CompressedSupportFiles"][2]["Value"], pipelineFwk)
        self.assertTrue(filecmp.cmp(pipelineFwk + ".decompressed", './pipelinefwk.py'))

        pipelineStepsScript = "./results/pipelinesteps.py.gz"
        self.decodeBase64ThenDecompressToFile(jsonDict["Model"]["CompressedSupportFiles"][3]["Value"], pipelineStepsScript)
        self.assertTrue(filecmp.cmp(pipelineStepsScript + ".decompressed", './lepipeline.tar.gz/pipelinesteps.py'))

        encoderScript = "./results/encoder.py.gz"
        self.decodeBase64ThenDecompressToFile(jsonDict["Model"]["CompressedSupportFiles"][4]["Value"], encoderScript)
        self.assertTrue(filecmp.cmp(encoderScript + ".decompressed", './lepipeline.tar.gz/encoder.py'))

        self.assertTrue(jsonDict["Model"]["Script"] is not None)
