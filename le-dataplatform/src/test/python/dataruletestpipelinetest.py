import sys

from dataruletestbase import DataRuleTestBase
from datarulestest.testrowrule import TestRowRule

class DataRuleTestPipelineTest(DataRuleTestBase):

    def testExecuteRulePipeline(self):
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher

        launcher = Launcher("model-datarule-test.json")
        launcher.execute(False)

        self.assertRuleOutputCount(8)
        self.assertColumnRuleOutput("./results/datarules/TestColumnRule_ColumnRule.avro", ["BW_mapping", "BW_seo_title"])
        testRowRule = TestRowRule()
        self.assertRowRuleOutput("./results/datarules/TestRowRule_RowRule.avro", testRowRule.getRowsToRemove())

        self.assertStandardRuleOutputs()