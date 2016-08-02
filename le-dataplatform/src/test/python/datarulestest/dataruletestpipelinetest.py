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

        self.assertRuleOutputCount(1)

