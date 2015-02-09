import json
from leframework.executors.learningexecutor import LearningExecutor
from profilingtestbase import ProfilingTestBase


class NullMedianProfileTest(ProfilingTestBase):

    def testExecuteProfiling(self):
        from launcher import Launcher
        profilinglauncher = Launcher("profiledriver-okta.json")
        profilinglauncher.execute(False, postProcessClf=False)
        learningExecutor = LearningExecutor()

        results = learningExecutor.retrieveMetadata("./results/profile.avro", False)
        for key in results[0]:
            value = results[0][key]
            for v in value:
                if v["Dtype"] == "BND":
                    self.assertTrue(v["median"] is not None)
                self.assertTrue(v["category"] is not None)
                self.assertTrue(v["displayname"] is not None)
                self.assertTrue(v["approvedusage"] is not None)

        # Show that the top predictor data type is different from schema data type
        predictor = results[0]["Intelliscore"]
        self.assertEquals(predictor[0]["Dtype"], "BND")
        
        
        fields = json.loads(open("schema-okta.avsc", "rb").read())["fields"]
        
        for field in fields:
            if field["name"] == "Intelliscore":
                self.assertEquals(field["type"][0], "string")
    
            

