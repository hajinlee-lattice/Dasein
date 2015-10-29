'''
This class is the operation for Dante pages
'''
__author__ = 'nxu'
import Selenium2Library
import yaml
from Property import DanteEnvironments
import time
import os
def ParseYaml(filepath):
    try:
        content=open(filepath,'r')
        dict_yaml=yaml.load(content)
        return dict_yaml
    except Exception,e:
        print "Load Yaml failed!"
        print e.message
    finally:
        content.close()

class DantePageHelper(object):
    PageLocator=ParseYaml(DanteEnvironments.Conf_file_Dante_Page)
    DantePages=PageLocator["DantePage"]
    def __init__(self,sales_force_url=DanteEnvironments.Sales_Force_URL,salesforce_user=DanteEnvironments.Sales_Force_User,salefore_pwd=DanteEnvironments.Sales_Force_PWD,browertype=DanteEnvironments.Browser_Type):
        self.salesforcelogin=sales_force_url
        self.dante_user=salesforce_user
        self.dante_pwd=salefore_pwd
        print browertype
        self.browser_type=browertype
        self.sele_instance=Selenium2Library.Selenium2Library()
        self.Timeout='120s'

    def OpenURL(self,url_to_open):
        self.sele_instance.open_browser(url_to_open, self.browser_type)

    def WaitElement(self,e_xpath):
         self.sele_instance.wait_until_page_contains_element(e_xpath,self.Timeout)

    def GetElementText(self,e_xpath):
        return self.sele_instance.get_text(e_xpath)

    def ClickLink(self,e_xpath):
        self.sele_instance.click_link(e_xpath)

    def GetCountMatched(self,e_xpath):
        return self.sele_instance.get_matching_xpath_count(e_xpath)

    def LogInSaleForcePage(self):
        #print '===start to log in SalesForce==='
        self.sele_instance.open_browser(self.salesforcelogin, self.browser_type)
        self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["LogInPage"]["LoginButton"],self.Timeout)
        self.sele_instance.input_text(DantePageHelper.DantePages["LogInPage"]["Username"],self.dante_user)
        self.sele_instance.input_password(DantePageHelper.DantePages["LogInPage"]["Password"],self.dante_pwd)
        self.sele_instance.click_button(DantePageHelper.DantePages["LogInPage"]["LoginButton"])
        self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["HomePage"]["TabsContainer"],self.Timeout)
        self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["HomePage"]["AllTabsLink"],self.Timeout)
        #print '===Have log in SalesForce==='

    def OpenDanteListPage(self,D_Type='Lead',IF_Config=False):
        print '===start to open '+str(D_Type)+' List page==='
        if IF_Config:
            self.Configure_Dante_Package(D_Type)
        self.sele_instance.click_link(DantePageHelper.DantePages["HomePage"]["AllTabsLink"])
        self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["AllTabPage"]["TabsTable"],self.Timeout)
        if (D_Type=='Account'):
            self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["AllTabPage"]["AccountsLink"],self.Timeout)
            self.sele_instance.click_link(DantePageHelper.DantePages["AllTabPage"]["AccountsLink"])
            self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["AccountListPage"]["ListPane"],self.Timeout)
            self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["AccountListPage"]["ViewSelector"],self.Timeout)
            self.sele_instance.select_from_list(DantePageHelper.DantePages["AccountListPage"]["ViewSelector"],DantePageHelper.DantePages["AccountListPage"]["OptionValue"])
            self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["AccountListPage"]["ListPane"],self.Timeout)
            self.sele_instance.select_frame(DantePageHelper.DantePages["AccountListPage"]["Iframe"])
            #print 'selected frame'
            #print DantePageHelper.DantePages["VisualAccountPage"]["LatticeTitle"]
            self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["VisualAccountPage"]["LatticeTitle"],self.Timeout)
            self.sele_instance.unselect_frame()
            self.sele_instance.click_element(DantePageHelper.DantePages["AccountListPage"]["DanteLink"])
            Account_FullID=self.sele_instance.get_text(DantePageHelper.DantePages["AccountListPage"]["FullID"])
            return Account_FullID
        elif (D_Type=='Contact'):
            self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["AllTabPage"]["ContactLink"],self.Timeout)
            self.sele_instance.click_link(DantePageHelper.DantePages["AllTabPage"]["ContactLink"])
            self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["ContactListPage"]["ListPane"],self.Timeout)
            self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["ContactListPage"]["ViewSelector"],self.Timeout)
            self.sele_instance.select_from_list(DantePageHelper.DantePages["ContactListPage"]["ViewSelector"],DantePageHelper.DantePages["ContactListPage"]["OptionValue"])
            self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["ContactListPage"]["ListPane"],self.Timeout)
            Contact_FullID=self.sele_instance.get_text(DantePageHelper.DantePages["ContactListPage"]["FullID"])
            return Contact_FullID
        elif (D_Type=='Lead'):
            self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["AllTabPage"]["LeadsLink"],self.Timeout)
            #print DantePageHelper.DantePages["AllTabPage"]["LeadsLink"]
            self.sele_instance.click_link(DantePageHelper.DantePages["AllTabPage"]["LeadsLink"])
            self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["LeadListPage"]["ListPane"],self.Timeout)
            self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["LeadListPage"]["ViewSelector"],self.Timeout)
            self.sele_instance.select_from_list(DantePageHelper.DantePages["LeadListPage"]["ViewSelector"],DantePageHelper.DantePages["LeadListPage"]["OptionValue"])
            self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["LeadListPage"]["ListPane"],self.Timeout)
            Lead_FullID=self.sele_instance.get_text(DantePageHelper.DantePages["LeadListPage"]["FullID"])
            return Lead_FullID
        print '===Have opened '+str(D_Type)+' List page==='

    def OpenDetailPage(self,D_Type):
        Full_ID=self.OpenDanteListPage(D_Type)
        print '===start to open '+str(D_Type)+' Detail page==='
        if (D_Type=='Account'):
            self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["AccountListPage"]["FullName"],self.Timeout)
            self.sele_instance.click_link(DantePageHelper.DantePages["AccountListPage"]["FullName"])
            self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["AccountDetailPage"]["DetailTable"],self.Timeout)
        elif (D_Type=='Lead'):
            self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["LeadListPage"]["FullName"],self.Timeout)
            self.sele_instance.click_link(DantePageHelper.DantePages["LeadListPage"]["FullName"])
            self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["LeadDetailPage"]["DetailTable"],self.Timeout)
        elif (D_Type=='Contact'):
            self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["ContactListPage"]["FullName"],self.Timeout)
            self.sele_instance.click_link(DantePageHelper.DantePages["ContactListPage"]["FullName"])
            self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["ContactDetailPage"]["DetailTable"],self.Timeout)
        return Full_ID
        print '===Have opened '+str(D_Type)+' Detail page==='

    def OpenConfigurePage(self,D_Type='Lead'):
        self.sele_instance.wait_until_page_contains_element('userNavLabel',self.Timeout)
        self.sele_instance.click_element('userNavLabel')
        self.sele_instance.wait_until_page_contains_element('userNavMenu',self.Timeout)
        self.sele_instance.click_link('Setup')
        self.sele_instance.wait_until_page_contains_element('DevToolsIntegrate',self.Timeout)
        self.sele_instance.click_link('DevToolsIntegrate_icon')
        self.sele_instance.wait_until_page_contains_element('CustomSettings_font',self.Timeout)
        self.sele_instance.click_link('CustomSettings_font')
        if (D_Type=='Lead'):
            self.sele_instance.wait_until_page_contains_element('Xpath=//a[contains(text(),"Lattice For Leads Configuration")]',self.Timeout)
            self.sele_instance.click_link('Xpath=//a[contains(text(),"Lattice For Leads Configuration")]')
        elif (D_Type=='Account'):
            self.sele_instance.wait_until_page_contains_element('Xpath=//a[contains(text(),"Lattice For Accounts Configuration")]',self.Timeout)
            self.sele_instance.click_link('Xpath=//a[contains(text(),"Lattice For Accounts Configuration")]')
        else:
            return
        self.sele_instance.wait_until_page_contains_element('CS_Defn_View:CS_View:theDetailBlock:detailButtons:manage',self.Timeout)
        self.sele_instance.click_button('CS_Defn_View:CS_View:theDetailBlock:detailButtons:manage')
        self.sele_instance.wait_until_page_contains_element('CS_list:CS_Form:theDetailPageBlock:thePageBlockButtons:edit',self.Timeout)
        self.sele_instance.click_button('CS_list:CS_Form:theDetailPageBlock:thePageBlockButtons:edit')

    def SetDanteServiceURL(self,D_Type='Lead',LatticeURL=DanteEnvironments.Sales_Force_DT_service,showlift=True,showScore=True,showRating=True):
        self.LogInSaleForcePage()
        self.OpenConfigurePage(D_Type)
        print '===end to open configuration page==='
        if (D_Type=='Lead'):
            lattice_url_id='CS_Edit:CS_Form:thePageBlock:thePageBlockSection:latticeforleads__url__c'
            show_lift_id='CS_Edit:CS_Form:thePageBlock:thePageBlockSection:latticeforleads__show_lift__c'
            show_score_id='CS_Edit:CS_Form:thePageBlock:thePageBlockSection:latticeforleads__show_score__c'
            show_rating_id='CS_Edit:CS_Form:thePageBlock:thePageBlockSection:latticeforleads__show_lattice_rating__c'
            self.sele_instance.wait_until_page_contains_element(lattice_url_id,self.Timeout)
            if showRating:
                self.sele_instance.select_checkbox(show_rating_id)
            else:
                self.sele_instance.unselect_checkbox(show_rating_id)
        elif (D_Type=='Account'):
            lattice_url_id='CS_Edit:CS_Form:thePageBlock:thePageBlockSection:lattice__url__c'
            show_lift_id='CS_Edit:CS_Form:thePageBlock:thePageBlockSection:lattice__show_lift__c'
            show_score_id='CS_Edit:CS_Form:thePageBlock:thePageBlockSection:lattice__show_score__c'
            self.sele_instance.wait_until_page_contains_element(lattice_url_id,self.Timeout)
        self.sele_instance.input_text(lattice_url_id,LatticeURL)
        if showlift:
             self.sele_instance.select_checkbox(show_lift_id)
        else:
             self.sele_instance.unselect_checkbox(show_lift_id)
        if showScore:
             self.sele_instance.select_checkbox(show_score_id)
        else:
             self.sele_instance.unselect_checkbox(show_score_id)
        self.sele_instance.click_button('CS_Edit:CS_Form:thePageBlock:thePageBlockButtons:save')
        self.sele_instance.wait_until_page_contains_element('CS_list:CS_Form:theDetailPageBlock:thePageBlockButtons:edit')
        self.Close_browser()

    def GetScoreInfo(self,D_Type='Lead',ISDetailPage=False):
        if ISDetailPage:
            if (D_Type=='Lead'):
                self.sele_instance.select_frame(DantePageHelper.DantePages["LeadDetailPage"]["Iframe_1"])
            elif (D_Type=='Contact'):
                self.sele_instance.select_frame(DantePageHelper.DantePages["ContactDetailPage"]["Iframe_1"])
            else:
                print 'Incorrect Dante Type, only support "Lead" and "Contact"'
                return
        if (D_Type=='Lead'):
            self.sele_instance.select_frame(DantePageHelper.DantePages["LeadListPage"]["Iframe"])
        elif (D_Type=='Contact'):
            self.sele_instance.select_frame(DantePageHelper.DantePages["ContactListPage"]["Iframe"])
        time.sleep(2)
        self.sele_instance.wait_until_page_contains_element('//div[@id="mainView"]',self.Timeout)
        self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["VisualLeadPage"]["LatticeTitle"],self.Timeout)
        lead_Display_Name=self.sele_instance.get_text(DantePageHelper.DantePages["VisualLeadPage"]["LatticeTitle"])
        self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["VisualLeadPage"]["ContentArea"],self.Timeout)
        #Score_Title and Score_Value be blocked by the bug ENG-7724, after it fixed we need to get the correct xpath to get them.
        #Score_Title='Lead Scoring Model'
        #Score_Title=self.sele_instance.get_text(DantePageHelper.DantePages["VisualLeadPage"]["ScoreTitle"])
        #Score_Value='77Score'
        Score_Value=self.sele_instance.get_text(DantePageHelper.DantePages["VisualLeadPage"]["ScoreValue"])
        self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["VisualLeadPage"]["External_buy_header"],self.Timeout)
        self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["VisualLeadPage"]["Internal_buy_title"],self.Timeout)
        External_Header= self.sele_instance.get_text(DantePageHelper.DantePages["VisualLeadPage"]["External_buy_header"])
        Internal_Header= self.sele_instance.get_text(DantePageHelper.DantePages["VisualLeadPage"]["Internal_buy_title"])
        if ISDetailPage:
            self.sele_instance.unselect_frame()
        self.sele_instance.unselect_frame()
        return lead_Display_Name,Score_Value,External_Header,Internal_Header

    def GetAccountPlay(self,ISDetailPage=False):
        print '===start to get info for play:==='
        result_dict={}
        if ISDetailPage:
            self.sele_instance.select_frame(DantePageHelper.DantePages["AccountDetailPage"]["Iframe_1"])
        self.sele_instance.select_frame(DantePageHelper.DantePages["AccountListPage"]["Iframe"])
        self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["VisualAccountPage"]["LatticeTitle"],self.Timeout)
        account_Title = self.sele_instance.get_text(DantePageHelper.DantePages["VisualAccountPage"]["LatticeTitle"])
        self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["VisualAccountPage"]["PlayList"],self.Timeout)
        play_count=self.sele_instance.get_matching_xpath_count(DantePageHelper.DantePages["VisualAccountPage"]["PlayList"])
        result_dict["DisplayName"]=account_Title
        plays_list=[]
        for play_index in range(1,int(play_count)+1):
            play_dict={}
            Play_Title_Xpath=str(DantePageHelper.DantePages["VisualAccountPage"]["PlayTitle"]).replace('Play_Index_Temp',str(play_index))
            self.sele_instance.wait_until_page_contains_element(Play_Title_Xpath,self.Timeout)
            play_title= self.sele_instance.get_text(Play_Title_Xpath)
            play_dict["P_Title"]=play_title
            Play_Score_Xpath=str(DantePageHelper.DantePages["VisualAccountPage"]["PlayScore"]).replace('Play_Index_Temp',str(play_index))
            self.sele_instance.wait_until_page_contains_element(Play_Score_Xpath,self.Timeout)
            play_score= self.sele_instance.get_text(Play_Score_Xpath)
            play_dict["P_Score"]=play_score
            time.sleep(1)
            self.sele_instance.click_element(Play_Title_Xpath)
            tp_list=self.GetTalkingPointsForPlay(ISDetailPage)
            play_dict["TalkingPoints"]=tp_list
            plays_list.append(play_dict)
        result_dict["Plays"]=plays_list
        if ISDetailPage:
            self.sele_instance.unselect_frame()
        self.sele_instance.unselect_frame()
        return result_dict

    def GetTalkingPointsForPlay(self,ISDetailPage=False):
        time.sleep(2)
        self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["VisualAccountPage"]["PlayDetail"],self.Timeout)
        self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["VisualAccountPage"]["TPHeaderArea"],self.Timeout)
        TP_Result_List=self.GetTalkingPoints()
        self.sele_instance.unselect_frame()
        #Following logic to return play list page is a work around for bug ENG-7716, after it fixed ,those code can be removed.
        if ISDetailPage:
            self.sele_instance.unselect_frame()
            self.sele_instance.reload_page()
            self.sele_instance.select_frame(DantePageHelper.DantePages["AccountDetailPage"]["Iframe_1"])
        else:
            #print '---start return play list---'
            self.sele_instance.click_element(DantePageHelper.DantePages["AccountListPage"]["DanteLink"])
        self.sele_instance.select_frame(DantePageHelper.DantePages["AccountListPage"]["Iframe"])
        return TP_Result_List

    def GetTalkingPoints(self):
        self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["VisualAccountPage"]["TPLink"],self.Timeout)
        #print 'Talk points lin exist in page'
        self.sele_instance.click_link(DantePageHelper.DantePages["VisualAccountPage"]["TPLink"])
        self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["VisualAccountPage"]["TPContentArea"],self.Timeout)
        #time.sleep(5)
        self.sele_instance.wait_until_page_contains_element('Xpath='+DantePageHelper.DantePages["VisualAccountPage"]["TPList"],self.Timeout)
        #print 'wait completed 5s'
        TP_count=self.sele_instance.get_matching_xpath_count(DantePageHelper.DantePages["VisualAccountPage"]["TPList"])
        #print str(TP_count)
        TP_Result_List=[]
        for talkpoints_index in range(1,int(TP_count)+1):
            TalkPoint_dic={}
            TP_Title_Xpath=str(DantePageHelper.DantePages["VisualAccountPage"]["TPTitle"]).replace('TP_Index_Temp',str(talkpoints_index))
            self.sele_instance.wait_until_page_contains_element(TP_Title_Xpath,self.Timeout)
            TP_Title= self.sele_instance.get_text(TP_Title_Xpath)
            TalkPoint_dic["Title"]=TP_Title
            #print TP_Title
            if talkpoints_index>1:
                #print 'click element for each title'
                #print str(talkpoints_index)
                self.sele_instance.click_element(TP_Title_Xpath)
            time.sleep(2)
            TPContent_Xpath=str(DantePageHelper.DantePages["VisualAccountPage"]["TPContent"]).replace('TP_Index_Temp',str(talkpoints_index))
            #print TPContent_Xpath
            self.sele_instance.wait_until_page_contains_element(TPContent_Xpath,self.Timeout)
            TP_Content= self.sele_instance.get_text(TPContent_Xpath)
            #print '-------------------------'
            #print TP_Content
            #print '========================='
            TalkPoint_dic["Content"]=TP_Content
            TP_Result_List.append(TalkPoint_dic)
            #print TalkPoint_dic
        #print TP_Result_List
        return sorted(TP_Result_List)

    def GetBuyingSignalsInfo(self):
        #self.sele_instance.()
        self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["VisualAccountPage"]["BS_External_Header"],self.Timeout)
        self.sele_instance.wait_until_page_contains_element(DantePageHelper.DantePages["VisualAccountPage"]["BS_Internal_Header"],self.Timeout)
        External_Header= self.sele_instance.get_text(DantePageHelper.DantePages["VisualAccountPage"]["BS_External_Header"])
        Internal_Header= self.sele_instance.get_text(DantePageHelper.DantePages["VisualAccountPage"]["BS_Internal_Header"])
        self.sele_instance.wait_until_page_contains_element('Xpath='+DantePageHelper.DantePages["VisualAccountPage"]["BS_External_List"],self.Timeout)
        self.sele_instance.wait_until_page_contains_element('Xpath='+DantePageHelper.DantePages["VisualAccountPage"]["BS_Internal_List"],self.Timeout)
        count_external=self.sele_instance.get_matching_xpath_count(DantePageHelper.DantePages["VisualAccountPage"]["BS_External_List"])
        count_internal=self.sele_instance.get_matching_xpath_count(DantePageHelper.DantePages["VisualAccountPage"]["BS_Internal_List"])
        External_info_dic={}
        External_info_dic["Title"]=External_Header
        Internal_info_dic={}
        Internal_info_dic["Title"]=Internal_Header
        External_attrs_list=[]
        for index_external in range(1,int(count_external)+1):
            Lattice_attrs_dic={}
            BS_Title_Xpath=str(DantePageHelper.DantePages["VisualAccountPage"]["BS_Attr_Title_External"]).replace('BS_Index_Temp',str(index_external))
            self.sele_instance.wait_until_page_contains_element(BS_Title_Xpath,self.Timeout)
            TP_Title= self.sele_instance.get_text(BS_Title_Xpath)
            Lattice_attrs_dic["Title"]=TP_Title
            BS_Description_Xpath=str(DantePageHelper.DantePages["VisualAccountPage"]["BS_Attr_Description_External"]).replace('BS_Index_Temp',str(index_external))
            self.sele_instance.wait_until_page_contains_element(BS_Description_Xpath,self.Timeout)
            BS_Description= self.sele_instance.get_text(BS_Description_Xpath)
            Lattice_attrs_dic["Description"]=BS_Description
            BS_Rate_Xpath=str(DantePageHelper.DantePages["VisualAccountPage"]["BS_Attr_Rate_External"]).replace('BS_Index_Temp',str(index_external))
            self.sele_instance.wait_until_page_contains_element(BS_Rate_Xpath,self.Timeout)
            BS_Rate= self.sele_instance.get_text(BS_Rate_Xpath)
            Lattice_attrs_dic["Rate"]=BS_Rate
            External_attrs_list.append(Lattice_attrs_dic)
        #print External_attrs_list
        External_info_dic["Attrs"]=sorted(External_attrs_list)
        Internal_attrs_list=[]
        for index_internal in range(1,int(count_internal)+1):
            Lattice_attrs_dic={}
            BS_Title_Xpath=str(DantePageHelper.DantePages["VisualAccountPage"]["BS_Attr_Title_Internal"]).replace('BS_Index_Temp',str(index_internal))
            self.sele_instance.wait_until_page_contains_element(BS_Title_Xpath,self.Timeout)
            TP_Title= self.sele_instance.get_text(BS_Title_Xpath)
            Lattice_attrs_dic["Title"]=TP_Title
            BS_Description_Xpath=str(DantePageHelper.DantePages["VisualAccountPage"]["BS_Attr_Description_Internal"]).replace('BS_Index_Temp',str(index_internal))
            self.sele_instance.wait_until_page_contains_element(BS_Description_Xpath,self.Timeout)
            BS_Description= self.sele_instance.get_text(BS_Description_Xpath)
            Lattice_attrs_dic["Description"]=BS_Description
            BS_Rate_Xpath=str(DantePageHelper.DantePages["VisualAccountPage"]["BS_Attr_Rate_Internal"]).replace('BS_Index_Temp',str(index_internal))
            self.sele_instance.wait_until_page_contains_element(BS_Rate_Xpath,self.Timeout)
            BS_Rate= self.sele_instance.get_text(BS_Rate_Xpath)
            Lattice_attrs_dic["Rate"]=BS_Rate
            Internal_attrs_list.append(Lattice_attrs_dic)
        #print Internal_attrs_list
        Internal_info_dic["Attrs"]=sorted(Internal_attrs_list)
        return External_info_dic,Internal_info_dic




    def Configure_Dante_Package(self,D_Type):
        print "test"

    def Close_browser(self):
        self.sele_instance.close_browser()
