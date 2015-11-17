
import glob
import json
import os
import sys

from leframework.executors.learningexecutor import LearningExecutor
from trainingtestbase import TrainingTestBase


class MulesoftProfilingThenTrainTest(TrainingTestBase):

    def testExecuteProfilingThenTrain(self):
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher

        # These properties won't really be used since these are just unit tests.
        # Functional and end-to-end tests should be done from java
        profilinglauncher = Launcher("profiledriver-mulesoft.json")
        profilinglauncher.execute(False, postProcessClf=False)
        learningExecutor = LearningExecutor()

        results = learningExecutor.retrieveMetadata("./results/profile.avro", False)
        self.assertTrue(results is not None)
        
        metadata = results[0]
        
        # Two values for a boolean
        self.assertEquals(len(metadata['MKTOLead_SpamIndicator']), 2)
        # Booleans should be categorical
        self.assertEquals(metadata['MKTOLead_SpamIndicator'][0]['Dtype'], 'STR')
        self.assertEquals(metadata['MKTOLead_SpamIndicator'][1]['Dtype'], 'STR')
        
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher
        
        os.symlink("./results/profile.avro", "profile-mulesoft.avro")

        traininglauncher = Launcher("modeldriver-mulesoft.json")
        traininglauncher.execute(False)
        
        jsonDict = json.loads(open(glob.glob("./results/*PLSModel*.json")[0]).read())
        
        rocScore = jsonDict["Summary"]["RocScore"]

        self.assertTrue(rocScore > 0.7)
        self.assertTrue(jsonDict["Model"]["Script"] is not None)
        self.assertTrue(jsonDict["NormalizationBuckets"] is not None)
        self.assertTrue(len(jsonDict["NormalizationBuckets"]) > 0)
        
        for k in jsonDict["InputColumnMetadata"]:
            if k["Name"] == "MKTOLead_SpamIndicator":
                self.assertEquals(k["ValueType"], 0)
                
        self.assertProfileNoColumnsWithApprovedUsageEqualNone(metadata)
        
    def assertProfileNoColumnsWithApprovedUsageEqualNone(self, metadata):
        columnsInProfile = set(metadata.keys())
        configMetadataFile = "./data/metadata-mulesoft.avsc"
        metadata = json.loads(open(configMetadataFile, "rb").read())["Metadata"]
        columnsInMetadata = set()
        for c in metadata:
            u = c["ApprovedUsage"]
            if isinstance(u, list) and u[0] == "None":
                columnsInMetadata.add(c["ColumnName"])
        
        self.assertEqual(len(columnsInMetadata.intersection(columnsInProfile)), 0)

    def tearDown(self):
        # Remove launcher module to restore its globals()
        del sys.modules['launcher']

