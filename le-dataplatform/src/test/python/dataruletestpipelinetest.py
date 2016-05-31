import sys

from dataruletestbase import DataRuleTestBase


class DataRuleTestPipelineTest(DataRuleTestBase):

    def testExecuteRulePipeline(self):
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher

        launcher = Launcher("model-datarule-test.json")
        launcher.execute(True)

        self.assertRuleOutputCount(2)
        self.assertRuleOutput("./results/datarules/TestColumnRule_Column.avro", ["AColumn", "CColumn"])
        self.assertRuleOutput("./results/datarules/TestRowRule_Row.avro", ["Row1", "Row2"])

