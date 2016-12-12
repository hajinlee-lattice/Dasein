import sys

from dataruletestbase import DataRuleTestBase

class DataRuleTestPipelineTest(DataRuleTestBase):

    def testExecuteRulePipeline(self):
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher

        launcher = Launcher("SlimEventTable_Mulesoft_Metadata_Review_20160624_155355.json")
        launcher.execute(False)
