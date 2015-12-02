import unittest

from Property import DanteEnvironments
from Libs.DanteUI.DantePageHelper import DantePageHelper
from Libs.Logger import Logger


class AccountInSFDCTest(unittest.TestCase):

    def testcase_1_VerifyBackToRecommodationlistInListPage(self):
        Logger.setInfoLog( '----test case: Verify Back to Recommondation list button can work in account list page----')
        page_Account_List=DantePageHelper(salesforce_user=DanteEnvironments.Sales_Force_User_Account,salefore_pwd=DanteEnvironments.Sales_Force_PWD_Account)
        page_Account_List.LogInSaleForcePage()
        full_ID=page_Account_List.OpenDanteListPage('Account')
        #Get the play count before we go to one play detail page
        page_Account_List.WaitElement('Xpath='+DantePageHelper.DantePages["VisualAccountPage"]["PlayList"])
        play_count_1=page_Account_List.GetCountMatched(DantePageHelper.DantePages["VisualAccountPage"]["PlayList"])
        #Go to one play detail page by click the play (we click the first play)
        result_dict=page_Account_List.GoToPlayDetailPage(1)
        #Click Back button to return to play list page and then get the play count
        page_Account_List.WaitElement(DantePageHelper.DantePages["VisualAccountPage"]["BackToPlayList"])
        page_Account_List.ClickLink(DantePageHelper.DantePages["VisualAccountPage"]["BackToPlayList"])
        page_Account_List.WaitElement('Xpath='+DantePageHelper.DantePages["VisualAccountPage"]["PlayList"])
        play_count_2=page_Account_List.GetCountMatched(DantePageHelper.DantePages["VisualAccountPage"]["PlayList"])
        page_Account_List.Close_browser()
        assert play_count_1==play_count_2, Logger.setErrorLog('The play count is different with the first time we access the recommendation list page and after we click back to recommendation list page.\nThe play count in recommendation list page first time is:\n------------- '+str(play_count_1)+'\n-------------\n'+'The play count after click back button is:\n------------- '+str(play_count_2)+'\n-------------\n')

    def testcase_2_VerifyBackToRecommodationlistInDetailPage(self):
        Logger.setInfoLog( '----test case: Verify Back to Recommondation list button can work in account detail page----')
        page_Account_Detail=DantePageHelper(salesforce_user=DanteEnvironments.Sales_Force_User_Account,salefore_pwd=DanteEnvironments.Sales_Force_PWD_Account)
        page_Account_Detail.LogInSaleForcePage()
        full_ID=page_Account_Detail.OpenDetailPage('Account')
        #Get the play count before we go to one play detail page
        page_Account_Detail.SelectOneIframe(DantePageHelper.DantePages["AccountDetailPage"]["Iframe_1"])
        page_Account_Detail.WaitElement('Xpath='+DantePageHelper.DantePages["VisualAccountPage"]["PlayList"])
        play_count_1=page_Account_Detail.GetCountMatched(DantePageHelper.DantePages["VisualAccountPage"]["PlayList"])
        page_Account_Detail.UnselectIframe()
        #Go to one play detail page by click the play (we click the first play)
        result_dict=page_Account_Detail.GoToPlayDetailPage(1,True)
        #Click Back button to return to play list page and then get the play count
        page_Account_Detail.WaitElement(DantePageHelper.DantePages["VisualAccountPage"]["BackToPlayList"])
        page_Account_Detail.ClickLink(DantePageHelper.DantePages["VisualAccountPage"]["BackToPlayList"])
        page_Account_Detail.WaitElement('Xpath='+DantePageHelper.DantePages["VisualAccountPage"]["PlayList"])
        play_count_2=page_Account_Detail.GetCountMatched(DantePageHelper.DantePages["VisualAccountPage"]["PlayList"])
        page_Account_Detail.Close_browser()
        assert play_count_1==play_count_2,  Logger.setErrorLog('The play count is different with the first time we access the recommendation list page and after we click back to recommendation list page.\nThe play count in recommendation list page first time is:\n------------- '+str(play_count_1)+'\n-------------\n'+'The play count after click back button is:\n------------- '+str(play_count_2)+'\n-------------\n')

    def testcase_3_VerifyActionsInDetailPage(self):
        Logger.setInfoLog( '----test case: Verify Action list button can work in account detail page----')
        expected_action_text_list=['Convert Opportunity','Link to Existing Opportunity','Disqualify','Send Email','Call','New Task','Set Up Meeting']
        page_Account_Detail=DantePageHelper(salesforce_user=DanteEnvironments.Sales_Force_User_Account,salefore_pwd=DanteEnvironments.Sales_Force_PWD_Account)
        page_Account_Detail.LogInSaleForcePage()
        full_ID=page_Account_Detail.OpenDetailPage('Account')
        #Get the play count before we go to one play detail page
        link_text,action_count,action_text_list=page_Account_Detail.GetActionsForPlay(True)
        page_Account_Detail.GoToPlayDetailPage(1,ISDetailPage=True)
        link_text_1,action_count_1,action_text_list_1=page_Account_Detail.GetActionsForPlay(ISPlayDetailPage=True,ISAccountDetailPage=True)
        page_Account_Detail.Close_browser()
        assert link_text=='Actions', Logger.setErrorLog('The text for Action button should be "Actions", but actually value in page is: "'+str(link_text)+'"')
        result=cmp(sorted(expected_action_text_list),action_text_list)
        assert result==0, Logger.setErrorLog("The text for action list from Page is not expected, Actully Result in page is \n----------------------\n" + str(action_text_list) + "\n-------------------------\n But Expected Result is \n-----------------\n" + str(expected_action_text_list) + "\n-----------------")
        assert link_text_1=='Actions', Logger.setErrorLog('The text for Action button should be "Actions", but actually value in page is: "'+str(link_text_1)+'"')
        result1=cmp(sorted(expected_action_text_list),action_text_list_1)
        assert result1==0, Logger.setErrorLog("The text for action list from Page is not expected, Actully Result in page is \n----------------------\n" + str(action_text_list_1) + "\n-------------------------\n But Expected Result is \n-----------------\n" + str(expected_action_text_list) + "\n-----------------")


    def testcase_4_VerifyActionsInListPage(self):
        Logger.setInfoLog( '----test case: Verify Actions in account list page----')
        expected_action_text_list=['Convert Opportunity','Link to Existing Opportunity','Disqualify','Send Email','Call','New Task','Set Up Meeting']
        page_Account_List=DantePageHelper(salesforce_user=DanteEnvironments.Sales_Force_User_Account,salefore_pwd=DanteEnvironments.Sales_Force_PWD_Account)
        page_Account_List.LogInSaleForcePage()
        full_ID=page_Account_List.OpenDanteListPage('Account')
        #Get the play count before we go to one play detail page
        link_text,action_count,action_text_list=page_Account_List.GetActionsForPlay()
        page_Account_List.GoToPlayDetailPage(1)
        link_text_1,action_count_1,action_text_list_1=page_Account_List.GetActionsForPlay(ISPlayDetailPage=True)
        page_Account_List.Close_browser()
        assert link_text=='Actions', Logger.setErrorLog('The text for Action button should be "Actions", but actually value in page is: "'+str(link_text)+'"')
        result=cmp(sorted(expected_action_text_list),action_text_list)
        assert result==0, Logger.setErrorLog("The text for action list from Page is not expected, Actully Result in page is \n----------------------\n" + str(action_text_list) + "\n-------------------------\n But Expected Result is \n-----------------\n" + str(expected_action_text_list) + "\n-----------------")
        assert link_text_1=='Actions', Logger.setErrorLog('The text for Action button should be "Actions", but actually value in page is: "'+str(link_text_1)+'"')
        result1=cmp(sorted(expected_action_text_list),action_text_list_1)
        assert result1==0, Logger.setErrorLog("The text for action list from Page is not expected, Actully Result in page is \n----------------------\n" + str(action_text_list_1) + "\n-------------------------\n But Expected Result is \n-----------------\n" + str(expected_action_text_list) + "\n-----------------")


if __name__ == "__main__":
    unittest.main()
