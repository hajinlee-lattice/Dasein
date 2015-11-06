import unittest
from Property import DanteEnvironments
from Operations.DantePageHelper import DantePageHelper
from Operations.DanteDataHelper import DanteDataHelper

class LeadInSFDCTest(unittest.TestCase):

    def testcase_1_LeadDetailPage(self):
        print '----test  case for Lead Detail page----'
        dp_L_Detail=DantePageHelper()
        ddh_ld=DanteDataHelper()
        dp_L_Detail.LogInSaleForcePage()
        full_ID=dp_L_Detail.OpenDetailPage('Lead')
        lead_dic=ddh_ld.ParseLeadScore(full_ID)
        Lead_infos_page=dp_L_Detail.GetScoreInfo('Lead',True)
        #print Lead_infos_page
        dp_L_Detail.Close_browser()
        assert str(Lead_infos_page[0])==str(lead_dic["Title"]),'Display Name in Lead Dante Page is not right, actually is: "'+str(Lead_infos_page[0])+'" ,but expected should be: "'+str(lead_dic["Title"])+'" '
        #assert str(Lead_infos_page[1])=="Lead Scoring Model", 'Score Title in Page: "'+str(Lead_infos_page[1])+'" is not equal: "Lead Scoring Model"'
        assert str(Lead_infos_page[1])==str(lead_dic["Score"]), 'Score in page is: "'+str(Lead_infos_page[1])+'", but should be: "'+str(lead_dic["Score"])+'"'
        assert str(Lead_infos_page[2])=="LATTICE ATTRIBUTES", 'Score Title in Page: "'+str(Lead_infos_page[2])+'" is not equal: "LATTICE ATTRIBUTES"'
        assert str(Lead_infos_page[3])=="INTERNAL ATTRIBUTES", 'Score Title in Page: "'+str(Lead_infos_page[3])+'" is not equal: "INTERNAL ATTRIBUTES"'

    def testcase_2_LeadListPage(self):
        print '----test  case for Lead list page----'
        dp_L_List=DantePageHelper()
        ddh_ll=DanteDataHelper()
        dp_L_List.LogInSaleForcePage()
        full_ID=dp_L_List.OpenDanteListPage('Lead')
        lead_dic=ddh_ll.ParseLeadScore(full_ID)
        lead_infos_page=dp_L_List.GetScoreInfo('Lead')
        #print lead_infos_page
        dp_L_List.Close_browser()
        assert str(lead_infos_page[0])==str(lead_dic["Title"]),'Display Name in Lead Dante Page is not right, actually is: "'+str(lead_infos_page[0])+'" ,but expected should be: "'+str(lead_dic["Title"])+'" '
        #assert str(lead_infos_page[1])=="Lead Scoring Model", 'Score Title in Page: "'+str(lead_infos_page[1])+'" is not equal: "Lead Scoring Model"'
        assert str(lead_infos_page[1])==str(lead_dic["Score"]), 'Score in page is: "'+str(lead_infos_page[1])+'", but should be: "'+str(lead_dic["Score"])+'"'
        assert str(lead_infos_page[2])=="LATTICE ATTRIBUTES", 'Score Title in Page: "'+str(lead_infos_page[2])+'" is not equal: "LATTICE ATTRIBUTES"'
        assert str(lead_infos_page[3])=="INTERNAL ATTRIBUTES", 'Score Title in Page: "'+str(lead_infos_page[3])+'" is not equal: "INTERNAL ATTRIBUTES"'

    def testcase_3_ContactListPage(self):
        print '----test  case for Contact list page----'
        dp_C_List=DantePageHelper()
        ddh_cl=DanteDataHelper()
        dp_C_List.LogInSaleForcePage()
        full_ID=dp_C_List.OpenDanteListPage('Contact')
        lead_dic=ddh_cl.ParseLeadScore(full_ID)
        lead_infos_page=dp_C_List.GetScoreInfo('Contact')
        #print Lead_infos_page
        dp_C_List.Close_browser()
        assert str(lead_infos_page[0])==str(lead_dic["Title"]),'Display Name in Lead Dante Page is not right, actually is: "'+str(lead_infos_page[0])+'" ,but expected should be: "'+str(lead_dic["Title"])+'" '
        #assert str(lead_infos_page[1])=="Lead Scoring Model", 'Score Title in Page: "'+str(lead_infos_page[1])+'" is not equal: "Lead Scoring Model"'
        assert str(lead_infos_page[1])==str(lead_dic["Score"]), 'Score in page is: "'+str(lead_infos_page[1])+'", but should be: "'+str(lead_dic["Score"])+'"'
        assert str(lead_infos_page[2])=="LATTICE ATTRIBUTES", 'Score Title in Page: "'+str(lead_infos_page[2])+'" is not equal: "LATTICE ATTRIBUTES"'
        assert str(lead_infos_page[3])=="INTERNAL ATTRIBUTES", 'Score Title in Page: "'+str(lead_infos_page[3])+'" is not equal: "INTERNAL ATTRIBUTES"'

    def testcase_4_ContactDetailPage(self):
        print '----test  case for Contact Detail page----'
        #print 'This case is blocked by the bug ENG-7677'
        dp_C_Detail=DantePageHelper()
        ddh_cd=DanteDataHelper()
        dp_C_Detail.LogInSaleForcePage()
        full_ID=dp_C_Detail.OpenDetailPage('Contact')
        lead_dic=ddh_cd.ParseLeadScore(full_ID)
        Lead_infos_page=dp_C_Detail.GetScoreInfo('Contact',True)
        #print Lead_infos_page
        dp_C_Detail.Close_browser()
        assert str(Lead_infos_page[0])==str(lead_dic["Title"]),'Display Name in Lead Dante Page is not right, actually is: "'+str(Lead_infos_page[0])+'" ,but expected should be: "'+str(lead_dic["Title"])+'" '
        #assert str(Lead_infos_page[1])=="Lead Scoring Model", 'Score Title in Page: "'+str(Lead_infos_page[1])+'" is not equal: "Lead Scoring Model"'
        assert str(Lead_infos_page[1])==str(lead_dic["Score"]), 'Score in page is: "'+str(Lead_infos_page[1])+'", but should be: "'+str(lead_dic["Score"])+'"'
        assert str(Lead_infos_page[2])=="LATTICE ATTRIBUTES", 'Score Title in Page: "'+str(Lead_infos_page[2])+'" is not equal: "LATTICE ATTRIBUTES"'
        assert str(Lead_infos_page[3])=="INTERNAL ATTRIBUTES", 'Score Title in Page: "'+str(Lead_infos_page[3])+'" is not equal: "INTERNAL ATTRIBUTES"'

    def testcase_5_LeadDanteWithoutScore(self):
        print '----test  case for Lead list page without score be configured----'
        dp_L_List_No_Score=DantePageHelper()
        dp_L_List_No_Score.SetDanteServiceURL('Lead',DanteEnvironments.Sales_Force_DT_service,True,False,False)
        ddh_ll=DanteDataHelper()
        dp_L_List_No_Score.LogInSaleForcePage()
        full_ID=dp_L_List_No_Score.OpenDanteListPage('Lead')
        lead_dic=ddh_ll.ParseLeadScore(full_ID)
        lead_infos_page=dp_L_List_No_Score.GetScoreInfo('Lead')
        dp_L_List_No_Score.Close_browser()
        dp_L_List_No_Score.SetDanteServiceURL('Lead',DanteEnvironments.Sales_Force_DT_service,True,True,True)
        assert str(lead_infos_page[0])==str(lead_dic["Title"]),'Display Name in Lead Dante Page is not right, actually is: "'+str(lead_infos_page[0])+'" ,but expected should be: "'+str(lead_dic["Title"])+'" '
        #assert str(lead_infos_page[1])=="Lead Scoring Model", 'Score Title in Page: "'+str(lead_infos_page[1])+'" is not equal: "Lead Scoring Model"'
        assert str(lead_infos_page[1])=='', 'Score in page is: "'+str(lead_infos_page[1])+'", but should be: ""'

    def testcase_6_LeadDanteWithRate(self):
        print '----test  case for Lead list page without Rate be configured----'
        dp_L_List_Rate=DantePageHelper()
        dp_L_List_Rate.SetDanteServiceURL('Lead',DanteEnvironments.Sales_Force_DT_service,True,False,True)
        ddh_ll=DanteDataHelper()
        dp_L_List_Rate.LogInSaleForcePage()
        full_ID=dp_L_List_Rate.OpenDanteListPage('Lead')
        lead_dic=ddh_ll.ParseLeadScore(full_ID)
        lead_infos_page=dp_L_List_Rate.GetScoreInfo('Lead')
        dp_L_List_Rate.Close_browser()
        dp_L_List_Rate.SetDanteServiceURL('Lead',DanteEnvironments.Sales_Force_DT_service,True,True,True)
        assert str(lead_infos_page[0])==str(lead_dic["Title"]),'Display Name in Lead Dante Page is not right, actually is: "'+str(lead_infos_page[0])+'" ,but expected should be: "'+str(lead_dic["Title"])+'" '
        #assert str(lead_infos_page[1])=="Lead Scoring Model", 'Score Title in Page: "'+str(lead_infos_page[1])+'" is not equal: "Lead Scoring Model"'
        assert str(lead_infos_page[1])==lead_dic["Rate"], 'Score in page is: "'+str(lead_infos_page[1])+'", but should be: "'+str(lead_dic["Rate"])+'"'

class AccountInSFDCTest(unittest.TestCase):

    def testcase_1_AccountListPage(self):
        print '----test case for account list page----'
        dp_A_list=DantePageHelper(salesforce_user=DanteEnvironments.Sales_Force_User_Account,salefore_pwd=DanteEnvironments.Sales_Force_PWD_Account)
        ddh_AL=DanteDataHelper()
        dp_A_list.LogInSaleForcePage()
        full_ID=dp_A_list.OpenDanteListPage('Account')
        #account_dict=ddh_AL.ParseAccountsPlay(full_ID)
        result_dict=dp_A_list.GetAccountPlay()
        dp_A_list.Close_browser()
        playlist=result_dict["Plays"]
        play_count=len(playlist)
        assert play_count>0,'No play be displayed in Dante UI'
        for play_index in range (1,int(play_count)+1):
            play_TPs=playlist[int(play_index)-1]["TalkingPoints"]
            TPs_count=len(play_TPs)
            assert TPs_count>0,'No Talking points attribute be displayed in Dante UI'
        #assert ddh_AL.CompareDanteDictionary(result_dict,account_dict),'account Dante data in page is not right, actually data is:   '+str(result_dict)+'   expected data is:   '+str(account_dict)

    def testcase_2_AccountDetailPage(self):
        print '----test case for account detail page----'
        dp_A_Detail=DantePageHelper(salesforce_user=DanteEnvironments.Sales_Force_User_Account,salefore_pwd=DanteEnvironments.Sales_Force_PWD_Account)
        #ddh_AD=DanteDataHelper()
        dp_A_Detail.LogInSaleForcePage()
        full_ID=dp_A_Detail.OpenDetailPage('Account')
        #account_dict=ddh_AD.ParseAccountsPlay(full_ID)
        result_dict=dp_A_Detail.GetAccountPlay(True)
        dp_A_Detail.Close_browser()
        playlist=result_dict["Plays"]
        play_count=len(playlist)
        assert play_count>0,'No play be displayed in Dante UI'
        for play_index in range (1,int(play_count)+1):
            play_TPs=playlist[int(play_index)-1]["TalkingPoints"]
            TPs_count=len(play_TPs)
            assert TPs_count>0,'No Talking points attribute be displayed in Dante UI'

        #assert ddh_AD.CompareDanteDictionary(result_dict,account_dict),'account Dante data in page is not right, actually data is:   '+str(result_dict)+'   expected data is:   '+str(account_dict)

    # def testcase_3_AccountWithoutScore(self):
    #     print '----test case for account list page without score be configured'
    #     dp_A_List_No_Score=DantePageHelper(salesforce_user=DanteEnvironments.Sales_Force_User_Account,salefore_pwd=DanteEnvironments.Sales_Force_PWD_Account)
    #     dp_A_List_No_Score.SetDanteServiceURL('Account',DanteEnvironments.Sales_Force_DT_service,True,False,False)
    #     ddh_al=DanteDataHelper()
    #     dp_A_List_No_Score.LogInSaleForcePage()
    #     full_ID=dp_A_List_No_Score.OpenDanteListPage('Account')
    #     dict_Page=dp_A_List_No_Score.GetAccountPlay()
    #     dp_A_List_No_Score.Close_browser()
    #     dp_A_List_No_Score.SetDanteServiceURL('Account',DanteEnvironments.Sales_Force_DT_service,True,True,True)
    #     for dic_item_page in dict_Page["Plays"]:
    #        assert dic_item_page['P_Score']=='','Play score for the play "'+dic_item_page["P_Title"]+'" in page should be empty, actually is: "'+str(dic_item_page["P_Score"])

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()