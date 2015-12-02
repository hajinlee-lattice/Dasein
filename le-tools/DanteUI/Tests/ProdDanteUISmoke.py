import unittest

from Property import DanteEnvironments
from Libs.DanteUI.DantePageHelper import DantePageHelper
from Libs.DanteUI.DanteDataHelper import DanteDataHelper
from Libs.Logger import Logger

class DanteDemoURLTest(unittest.TestCase):

    def testcase_1_VerifyTalkingPoints(self):
        #print '----PROD Dante test case for Talking Points----'
        Logger.setInfoLog('\n----Start PROD Dante test case for Talking Points----')
        dantepage_TalkingPoints = DantePageHelper()
        dantepage_TalkingPoints.OpenURL(DanteEnvironments.PROD_Dante_URL)
        dantepage_TalkingPoints.WaitElement('Xpath=//a[text()="Talking Points"]')
        dantepage_TalkingPoints.ClickLink('Xpath=//a[text()="Talking Points"]')
        infos_TalkingPoints_From_Page = dantepage_TalkingPoints.GetTalkingPoints()
        danteData_TalkingPoints = DanteDataHelper()
        infos_TalkingPoints_Expected = danteData_TalkingPoints.GetPRODExpectedTalkingPoints('.\Dante\Config\ExpectedResult.xml')
        dantepage_TalkingPoints.Close_browser()
        result = cmp(infos_TalkingPoints_From_Page, infos_TalkingPoints_Expected)
        assert result == 0, Logger.setErrorLog("Talking Points from Page is not same with expected Result, Actully Result is \n----------------------\n" + str(infos_TalkingPoints_From_Page) + "\n-------------------------\n But Expected Result is \n-----------------\n" + str(infos_TalkingPoints_Expected) + "\n-----------------")



    def testcase_2_VerifyBuyingSignal(self):
        Logger.setInfoLog( '\n----PROD Dante test case for Buying Signals----')
        dantePage__BuyingSignal = DantePageHelper()
        dantePage__BuyingSignal.OpenURL(DanteEnvironments.PROD_Dante_URL)
        dantePage__BuyingSignal.WaitElement('Xpath=//a[text()="Buying Signals"]')
        dantePage__BuyingSignal.ClickLink('Xpath=//a[text()="Buying Signals"]')
        infos_BuingSignal_From_Page = dantePage__BuyingSignal.GetBuyingSignalsInfo()
        danteData_BuyingSignal = DanteDataHelper()
        infos_BuingSignal_Expected = danteData_BuyingSignal.GetPRODExpectedBuyingSingal('.\Dante\Config\ExpectedResult.xml')
        dantePage__BuyingSignal.Close_browser()
        result = cmp(infos_BuingSignal_From_Page, infos_BuingSignal_Expected)
        assert result == 0, Logger.setErrorLog("Buying Signal from Page is not same with expected Result, Actully Result is \n----------------------\n" + str(infos_BuingSignal_From_Page) + "\n-------------------------\n But Expected Result is \n-----------------\n" + str(infos_BuingSignal_Expected) + "\n-----------------")

class DanteInSFDCTest(unittest.TestCase):

    @unittest.skip("")
    def testcase_1_VerifyTalkingPoints(self):
        Logger.setInfoLog( '----PROD Dante test case for Talking Points----')
        dantePage_TalkingPoints=DantePageHelper(salesforce_user=DanteEnvironments.Sales_Force_User_Account,salefore_pwd=DanteEnvironments.Sales_Force_PWD_Account)
        dantePage_TalkingPoints.LogInSaleForcePage()
        full_ID=dantePage_TalkingPoints.OpenDanteListPage('Account')
        result=dantePage_TalkingPoints.GoToPlayDetailPage(1)
        assert result, Logger.setErrorLog("Case failed because the play index not be found!")
        infos_TalkingPoints_From_Page=dantePage_TalkingPoints.GetTalkingPointsForPlay()
        danteData_TalkingPoints=DanteDataHelper()
        TP_infos_expected=danteData_TalkingPoints.GetPRODExpectedTalkingPoints('.\Dante\Config\ExpectedResult.xml')
        dantePage_TalkingPoints.Close_browser()
        #result=cmp(TP_infos_page,TP_infos_expected)
        #assert result==0, "Talking Points from Page is not same with expected Result, Actully Result is \n----------------------\n"+str(TP_infos_page)+"\n-------------------------\n But Expected Result is \n-----------------\n"+str(TP_infos_expected)+"\n-----------------"
        tps_count=len(infos_TalkingPoints_From_Page)
        assert tps_count>0,  Logger.setErrorLog('No Talking points be displayed')
        for tp_index in range (0,tps_count):
            title_TalkingPoint=infos_TalkingPoints_From_Page[tp_index]["Title"]
            content_TalkingPoints=infos_TalkingPoints_From_Page[tp_index]["Content"]
            assert ((title_TalkingPoint !='') or (title_TalkingPoint!=None)),Logger.setErrorLog('Talking Points Title not exist or is empty in page')
            assert (content_TalkingPoints!=None),Logger.setErrorLog('Talking Points Title not exist in page')

    @unittest.skip("")
    def testcase_2_VerifyBuyingSignal(self):
        Logger.setInfoLog( '----PROD Dante test case for Buying Signals----')
        dantePage__BuyingSignal=DantePageHelper(salesforce_user=DanteEnvironments.Sales_Force_User_Account,salefore_pwd=DanteEnvironments.Sales_Force_PWD_Account)
        dantePage__BuyingSignal.LogInSaleForcePage()
        full_ID=dantePage__BuyingSignal.OpenDetailPage('Account')
        result=dantePage__BuyingSignal.GoToPlayDetailPage(1,True)
        assert result,Logger.setErrorLog("Case failed because the play index not be found!")
        infos_BuingSignal_From_Page=dantePage__BuyingSignal.GetBuyingSignalForPlay(True)
        danteData_BuyingSignal=DanteDataHelper()
        infos_BuingSignal_Expected=danteData_BuyingSignal.GetPRODExpectedBuyingSingal('.\Dante\Config\ExpectedResult.xml')
        dantePage__BuyingSignal.Close_browser()
        assert len(infos_BuingSignal_From_Page)==2, Logger.setErrorLog('Internal and external attributes should be displayed in page,but now only '+str(len(infos_BuingSignal_From_Page)) +' area be displayed.')
        external_attrs=infos_BuingSignal_From_Page[0]["Attrs"]
        internal_attrs=infos_BuingSignal_From_Page[1]["Attrs"]
        assert len(external_attrs)>0,Logger.setErrorLog('No external attributes be diaplyed in page')
        assert len(internal_attrs)>0,Logger.setErrorLog('No internal attributes be diaplyed in page')
        #result=cmp(BS_infos_page,BS_infos_expected)
        #assert result==0, "Buying Signal from Page is not same with expected Result, Actully Result is \n----------------------\n"+str(BS_infos_page)+"\n-------------------------\n But Expected Result is \n-----------------\n"+str(BS_infos_expected)+"\n-----------------"



if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
