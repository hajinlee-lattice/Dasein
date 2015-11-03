import unittest
from Property import DanteEnvironments
from Operations.DantePageHelper import DantePageHelper
from Operations.DanteDataHelper import DanteDataHelper

class Test(unittest.TestCase):

    def testcase_1_VerifyTalkingPoints(self):
        print '----PROD Dante test case for Talking Points----'
        dp_TP = DantePageHelper()
        dp_TP.OpenURL(DanteEnvironments.PROD_Dante_URL)
        dp_TP.WaitElement('Xpath=//a[text()="Talking Points"]')
        dp_TP.ClickLink('Xpath=//a[text()="Talking Points"]')
        TP_infos_page = dp_TP.GetTalkingPoints()
        data_TP = DanteDataHelper()
        TP_infos_expected = data_TP.GetPRODExpectedTalkingPoints('.\Config\ExpectedResult.xml')
        dp_TP.Close_browser()
        result = cmp(TP_infos_page, TP_infos_expected)
        assert result == 0, "Talking Points from Page is not same with expected Result, Actully Result is \n----------------------\n" + str(TP_infos_page) + "\n-------------------------\n But Expected Result is \n-----------------\n" + str(TP_infos_expected) + "\n-----------------"


    def testcase_2_VerifyBuyingSignal(self):
        print '----PROD Dante test case for Buying Signals----'
        dp_BS = DantePageHelper()
        dp_BS.OpenURL(DanteEnvironments.PROD_Dante_URL)
        dp_BS.WaitElement('Xpath=//a[text()="Buying Signals"]')
        dp_BS.ClickLink('Xpath=//a[text()="Buying Signals"]')
        BS_infos_page = dp_BS.GetBuyingSignalsInfo()
        data_BS = DanteDataHelper()
        BS_infos_expected = data_BS.GetPRODExpectedBuyingSingal('.\Config\ExpectedResult.xml')
        dp_BS.Close_browser()
        result = cmp(BS_infos_page, BS_infos_expected)
        assert result == 0, "Buying Signal from Page is not same with expected Result, Actully Result is \n----------------------\n" + str(BS_infos_page) + "\n-------------------------\n But Expected Result is \n-----------------\n" + str(BS_infos_expected) + "\n-----------------"



if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
