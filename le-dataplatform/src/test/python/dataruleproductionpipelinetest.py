import sys

from dataruletestbase import DataRuleTestBase


class DataRuleProductionPipelineTest(DataRuleTestBase):

    def testExecuteRulePipeline(self):
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher

        launcher = Launcher("model-datarule.json")
        launcher.execute(False)

        self.assertRuleOutputCount(1)
        self.assertRuleOutput("./results/datarules/AnonymousLeadRule_Row.avro", ["34ds"])
