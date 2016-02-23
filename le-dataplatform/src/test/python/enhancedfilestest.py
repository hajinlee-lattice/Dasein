import os
import sys
import json

from trainingtestbase import TrainingTestBase
from uuid import UUID

class EnhancedFilesTest(TrainingTestBase):

    def tearDown(self):
        super(TrainingTestBase, self).tearDown()
        self.tearDownClass()
        self.setUpClass()

    def testEnhancedSummary(self):
        self.launch("model.json")
        self.check_model_summary()
        self.check_data_composition()
        self.check_score_derivation()

    def launch(self, model):
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules: del sys.modules['launcher']
        from launcher import Launcher
        launcher = Launcher(model)
        launcher.execute(False)
        launcher.training

    def check_model_summary(self):
        # Output File Exists?
        outputFile = "./results/enhancements/modelsummary.json"
        self.assertTrue(os.path.isfile(outputFile))

        # Load Summary
        summary = json.loads(open(outputFile).read())

        # Check Details
        details = summary["ModelDetails"]
        self.assertEqual(details["Name"], "Model_Submission1")
        self.assertIsNotNone(details["LookupID"])
        self.assertEqual(details["LookupID"].split("|")[0], "Nutanix")
        self.assertEqual(details["LookupID"].split("|")[1], "Q_EventTable_Nutanix")
        self.assertIsNotNone(UUID("{" + details["LookupID"].split("|")[2] + "}"))
        self.assertEqual(details["TotalLeads"], 8000)
        self.assertEqual(details["TestingLeads"], 2000)
        self.assertEqual(details["TrainingLeads"], 6000)
        self.assertEqual(details["TotalConversions"], 226)
        self.assertEqual(details["TestingConversions"], 36)
        self.assertEqual(details["TrainingConversions"], 190)
        self.assertIsNotNone(details["RocScore"])
        self.assertIsNotNone(details["ConstructionTime"])

        # Check Segmentations
        segments = summary["Segmentations"][0]["Segments"]
        count = reduce(lambda acc, e: acc + e["Count"], segments, 0)
        converted = reduce(lambda acc, e: acc + e["Converted"], segments, 0)
        self.assertEqual(count, details["TestingLeads"])
        self.assertEqual(converted, details["TestingConversions"])

        # Check Top Sample
        topSample = summary["TopSample"]
        self.assertEqual(len(topSample), 0)

        # Check Bottom Sample
        bottomSample = summary["BottomSample"]
        self.assertEqual(len(bottomSample), 0)

        # Check Provenance
        provenance = summary["EventTableProvenance"]
        self.assertEqual(provenance["SourceURL"], "http://10.41.1.238/")
        self.assertEqual(provenance["TenantName"], "ADEBD2V67059448rX25059174r")
        self.assertEqual(provenance["QueryName"], "DataForScoring_Lattice")

    def check_data_composition(self):
        # Output File Exists?        
        outputFile = "./results/enhancements/datacomposition.json"
        self.assertTrue(os.path.isfile(outputFile))

    def check_score_derivation(self):
        # Output File Exists?       
        outputFile = "./results/enhancements/scorederivation.json"
        self.assertTrue(os.path.isfile(outputFile))
