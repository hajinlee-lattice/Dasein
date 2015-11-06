import unittest
from Property import DanteEnvironments
from Operations.DantePageHelper import DantePageHelper
from Operations.DanteDataHelper import DanteDataHelper

class DanteDemoURLTest(unittest.TestCase):

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

class DanteInSFDCTest(unittest.TestCase):

    @unittest.skip("")
    def testcase_1_VerifyTalkingPoints(self):
        print '----PROD Dante test case for Talking Points----'
        dp_TP = DantePageHelper(salesforce_user=DanteEnvironments.Sales_Force_User_Account, salefore_pwd=DanteEnvironments.Sales_Force_PWD_Account)
        dp_TP.LogInSaleForcePage()
        full_ID = dp_TP.OpenDanteListPage('Account')
        result = dp_TP.GoToPlayDetailPage(1)
        assert result, "Case failed because the play index not be found!"
        TP_infos_page = dp_TP.GetTalkingPointsForPlay()
        data_TP = DanteDataHelper()
        TP_infos_expected = data_TP.GetPRODExpectedTalkingPoints('.\Config\ExpectedResult.xml')
        dp_TP.Close_browser()
        # result=cmp(TP_infos_page,TP_infos_expected)
        # assert result==0, "Talking Points from Page is not same with expected Result, Actully Result is \n----------------------\n"+str(TP_infos_page)+"\n-------------------------\n But Expected Result is \n-----------------\n"+str(TP_infos_expected)+"\n-----------------"
        TPs_count = len(TP_infos_page)
        assert TPs_count > 0, 'No Talking points be displayed'
        for tp_index in range (0, TPs_count):
            TP_Title = TP_infos_page[tp_index]["Title"]
            TP_content = TP_infos_page[tp_index]["Content"]
            assert ((TP_Title != '') or (TP_Title != None)), 'Talking Points Title not exist or is empty in page'
            assert (TP_content != None), 'Talking Points Title not exist in page'

    @unittest.skip("")
    def testcase_2_VerifyBuyingSignal(self):
        print '----PROD Dante test case for Buying Signals----'
        dp_BS = DantePageHelper(salesforce_user=DanteEnvironments.Sales_Force_User_Account, salefore_pwd=DanteEnvironments.Sales_Force_PWD_Account)
        dp_BS.LogInSaleForcePage()
        full_ID = dp_BS.OpenDetailPage('Account')
        result = dp_BS.GoToPlayDetailPage(1, True)
        assert result, "Case failed because the play index not be found!"
        BS_infos_page = dp_BS.GetBuyingSignalForPlay(True)
        data_BS = DanteDataHelper()
        BS_infos_expected = data_BS.GetPRODExpectedBuyingSingal('.\Config\ExpectedResult.xml')
        dp_BS.Close_browser()
        assert len(BS_infos_page) == 2, 'Internal and external attributes should be displayed in page,but now only ' + str(len(BS_infos_page)) + ' area be displayed.'
        external_attrs = BS_infos_page[0]["Attrs"]
        internal_attrs = BS_infos_page[1]["Attrs"]
        assert len(external_attrs) > 0, 'No external attributes be diaplyed in page'
        assert len(internal_attrs) > 0, 'No internal attributes be diaplyed in page'
        # result=cmp(BS_infos_page,BS_infos_expected)
        # assert result==0, "Buying Signal from Page is not same with expected Result, Actully Result is \n----------------------\n"+str(BS_infos_page)+"\n-------------------------\n But Expected Result is \n-----------------\n"+str(BS_infos_expected)+"\n-----------------"



if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
