import glob
import json
import os
import sys

from leframework.executors.learningexecutor import LearningExecutor
from trainingtestbase import TrainingTestBase

class SuiteProfilingThenTrainTest(TrainingTestBase):
    def executeProfilingThenTrain(self):
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher
        
        # These properties won't really be used since these are just unit tests.
        # Functional and end-to-end tests should be done from java
        profilinglauncher = Launcher("metadata-profile.json")
        profilinglauncher.execute(False)
        learningExecutor = LearningExecutor()
        results = learningExecutor.retrieveMetadata("./results/profile.avro", False)
        self.assertIsNotNone(results)
        
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher
        os.symlink("./results/profile.avro", "profile.avro")
        os.symlink("../resources/com/latticeengines/dataplatform/python/modelpredictorextraction.py", "modelpredictorextraction.py")
        traininglauncher = Launcher("metadata-model.json")
        traininglauncher.execute(False)
        jsonDict = json.loads(open(glob.glob("./results/PLSModel*.json")[0]).read())
        self.assertIsNotNone(jsonDict)
        self.assertModelOutput(results[0], jsonDict)
        
    def assertModelOutput(self, metadataInProfile, modelDict):
        self.assertTrue(modelDict.has_key("AverageProbability"))
        self.assertTrue(modelDict.has_key("Model"))
        self.assertTrue(modelDict.has_key("Name"))
        self.assertTrue(len(modelDict["Buckets"]) > 0)
        self.assertTrue(len(modelDict["InputColumnMetadata"]) > 0)
        
        self.assertTrue(modelDict["Summary"].has_key("SchemaVersion"))
        self.assertTrue(modelDict["Summary"].has_key("DLEventTableData"))
        self.assertTrue(modelDict["Summary"].has_key("ConstructionInfo"))
        self.assertTrue(len(modelDict["Summary"]["SegmentChart"]) > 0)
        self.assertTrue(len(modelDict["PercentileBuckets"]) > 0)
        predictors = modelDict["Summary"]["Predictors"]
        self.assertTrue(len(predictors) > 0)
        
        columnsInPredictors = set()
        columnsInProfile = set(metadataInProfile.keys())
        for predictor in predictors:
            columnsInPredictors.add(predictor["Name"])
        self.assertEqual(columnsInProfile, columnsInPredictors)
        
        configMetadataFile = "metadata.avsc"
        configMetadata = json.loads(open(configMetadataFile, "rb").read())["Metadata"]
        columnsInMetadata = set()
        for c in configMetadata:
            u = c["ApprovedUsage"]
            if isinstance(u, list) and u[0] == "None":
                columnsInMetadata.add(c["ColumnName"])
        self.assertEqual(len(columnsInMetadata.intersection(columnsInPredictors)), 0)
                         
    def tearDown(self):
        super(TrainingTestBase, self).tearDown()
        # Remove launcher module to restore its globals()
        del sys.modules['launcher']
        
class SuiteMuleSoftProfilingThenTrainTest(SuiteProfilingThenTrainTest):
    def testExecuteProfilingThenTrain(self):
        super(SuiteMuleSoftProfilingThenTrainTest, self).executeProfilingThenTrain()
        jsonDict = json.loads(open(glob.glob("./results/PLSModel*.json")[0]).read())
        rocScore = jsonDict["Summary"]["RocScore"]
        self.assertTrue(rocScore > 0.83)
        
    @classmethod
    def getSubDir(cls):
        return "Mulesoft_Relaunch"

class SuiteHirevueProfilingThenTrainTest(SuiteProfilingThenTrainTest):
    def testExecuteProfilingThenTrain(self):
        super(SuiteHirevueProfilingThenTrainTest, self).executeProfilingThenTrain()
        jsonDict = json.loads(open(glob.glob("./results/PLSModel*.json")[0]).read())
        rocScore = jsonDict["Summary"]["RocScore"]
        self.assertTrue(rocScore > 0.91)

    @classmethod
    def getSubDir(cls):
        return "PLS132_test_Hirevue"
    
class SuiteDocsignProfilingThenTrainTest(SuiteProfilingThenTrainTest):
    def testExecuteProfilingThenTrain(self):
        super(SuiteDocsignProfilingThenTrainTest, self).executeProfilingThenTrain()
        jsonDict = json.loads(open(glob.glob("./results/PLSModel*.json")[0]).read())
        rocScore = jsonDict["Summary"]["RocScore"]
        
        self.assertTrue(rocScore > 0.93)

    @classmethod
    def getSubDir(cls):
        return "PLS132_test_Docusign"

