import fastavro as avro
import glob
import os
import shutil

from trainingtestbase import TrainingTestBase

class DataRuleTestBase(TrainingTestBase):

    def setUp(self):
        super(DataRuleTestBase, self).setUp()
        os.symlink("../../main/python/rulefwk.py", "./rulefwk.py")
        os.symlink("../../main/python/datarules/rulepipeline.json", "rulepipeline.json")
        os.symlink("../../test/python/datarulestest/testrulepipeline.json", "testrulepipeline.json")
        for filename in glob.glob(os.path.join("../../main/python/datarules", "*.py")):
            shutil.copy(filename, self.pipelinefwkdir)
        for filename in glob.glob(os.path.join("../../test/python/datarulestest", "*.py")):
            shutil.copy(filename, self.pipelinefwkdir)


    def tearDown(self):
        super(DataRuleTestBase, self).tearDown()

    def assertRuleOutputCount(self, expectedCount):
        self.assertEqual(len(glob.glob("./results/datarules/*.avro")), expectedCount)

    def assertRuleOutput(self, ruleOutput, expectedColumns):
        actualColumns = []
        self.assertTrue(os.path.exists(ruleOutput))
        if os.path.isfile(ruleOutput):
            with open(ruleOutput) as fp:
                reader = avro.reader(fp)
                for record in reader:
                    actualColumns.append(record["itemid"])

        self.assertListEqual(expectedColumns, actualColumns, "")
