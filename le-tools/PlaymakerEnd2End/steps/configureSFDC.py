"""
@author bwang
@createDate 11/11/2015 
"""
import time
from PlaymakerEnd2End.Configuration.Properties import SalePrismEnvironments
from Libs.DanteUI.DantePageHelper import DantePageHelper
log=SalePrismEnvironments.log
class DealSFDC(object):
    def __init__(self):
        self.sf=DantePageHelper(sales_force_url=SalePrismEnvironments.SFurl,salesforce_user=SalePrismEnvironments.SFDCUser,salefore_pwd=SalePrismEnvironments.SFDCPWD,browertype=SalePrismEnvironments.driverType)
        self.driver=self.sf.sele_instance
        self.timeout='120s'
    def loginSF(self,user=SalePrismEnvironments.SFDCUser,pwd=SalePrismEnvironments.SFDCPWD):
        self.sf.LogInSaleForcePage()
        log.info("salesforce login successfully")
    def configDanteServer(self,dante_Server=SalePrismEnvironments.dante_Server):
        self.sf.SetDanteServiceURL(D_Type='Account',LatticeURL=dante_Server,defaultTab='TalkingPoints',hasSalesPrism=True)

    def configOTK(self,tenant=SalePrismEnvironments.tenantName):
        self.driver.wait_until_page_contains_element("Xpath=//a[text()='Lattice Admin']",self.timeout)
        self.driver.click_link("Xpath=//a[text()='Lattice Admin']")
        log.info( 'start to OAuth Connect')
        if self.driver._is_element_present("Xpath=//h3[text()='You are authenticated']"):
            log.info('App have authed before connect')
            self.driver.wait_until_page_contains_element("Xpath=//a[text()='Disconnect']",self.timeout)
            log.info('Start to disconnect Oauth first')
            self.driver.click_link("Xpath=//a[text()='Disconnect']")
            self.driver.wait_until_page_contains_element("Xpath=//input[@value='Disconnect']",self.timeout)
            self.driver.wait_until_page_contains_element("Xpath=//input[@value='Disconnect']",self.timeout)
            self.driver.wait_until_element_is_visible("Xpath=//input[@value='Disconnect']",self.timeout)
            log.info( 'disconnect button is visable, click it to disconnect')
            self.driver.click_button("Xpath=//input[@value='Disconnect']")
            log.info( 'disconnect completed')
            #self.driver.click_element("Xpath=//input[@value='Disconnect']")
        self.driver.wait_until_page_contains_element("Xpath=//a[text()='Authenticate Now']",self.timeout)
        log.info( 'click auth link')
        self.driver.click_link("Xpath=//a[text()='Authenticate Now']")
        self.driver.wait_until_page_contains_element("Xpath=//label[text()='Customer ID:']//parent::div//child::input",self.timeout)
        self.driver.wait_until_element_is_visible("Xpath=//label[text()='Customer ID:']//parent::div//child::input",self.timeout)
        self.driver.input_text("Xpath=//label[text()='Customer ID:']//parent::div//child::input",tenant)
        log.info( 'set tenant name')
        self.driver.wait_until_page_contains_element("Xpath=//label[text()='Token:']//parent::div//child::input",self.timeout)
        self.driver.wait_until_element_is_visible("Xpath=//label[text()='Token:']//parent::div//child::input",self.timeout)
        #self.driver.clear_element_text("Xpath=//label[text()='Token:']//parent::div//child::input")
        self.driver.input_text("Xpath=//label[text()='Token:']//parent::div//child::input",SalePrismEnvironments.OTK)
        log.info('set OTK')
        self.driver.wait_until_page_contains_element("Xpath=//input[@value='Connect']",self.timeout)
        self.driver.wait_until_element_is_visible("Xpath=//input[@value='Connect']",self.timeout)
        self.driver.click_element("Xpath=//input[@value='Connect']")
        log.info('connect tenant')
        time.sleep(10)
        #self.driver.wait_until_page_contains_element("Xpath=//a[text()='Lattice Admin']",self.timeout)
        #self.driver.click_link("Xpath=//a[text()='Lattice Admin']")
        self.driver.page_should_contain("You are authenticated")
        log.info("Authentication done!")
    def syncData(self):
        log.info('Go to Lattice Admin page')
        self.driver.wait_until_page_contains_element("Xpath=//a[text()='Lattice Admin']",self.timeout)
        self.driver.click_link("Xpath=//a[text()='Lattice Admin']")
        #self.driver.wait_until_page_contains_element("Xpath=//h3[text()='You are authenticated']",self.timeout)
        log.info('start to Sync data')
        self.driver.wait_until_page_contains_element("Xpath=//a[text()='Sync Data Now']",self.timeout)
        self.driver.click_link("Xpath=//a[text()='Sync Data Now']")
        self.driver.wait_until_page_contains_element("Xpath=//input[@value='Sync']",self.timeout)
        time.sleep(10)
        self.driver.wait_until_element_is_visible("Xpath=//input[@value='Sync']",self.timeout)
        self.driver.click_button("Xpath=//input[@value='Sync']")
        log.info('have set click button, wait for sync finished')
        time.sleep(5)
        self.driver.page_should_contain("The Sync Data job was initiated successfully")
        time.sleep(20)
        log.info ('syc successfully!')
    def checkRecommendations(self,playName,numberOfRecommendations=0):
        log.info('Go to Lattice Recommondation page')
        self.driver.wait_until_page_contains_element("Xpath=//a[text()='Lattice Recommendations']",self.timeout)
        self.driver.click_link("Xpath=//a[text()='Lattice Recommendations']")
        self.driver.wait_until_page_contains_element('Xpath=//div[@id="ext-gen11"]/div',self.timeout)
        is_recommendationExist = False
        num_recommendation=0
        num_page=1
		#pageNumber=int(self.driver.find_element_by_xpath("//span[text()='Page']").text[-1])
        next_page_is_enable=True
        while next_page_is_enable:
            #print 'this is page:'+str(num_page)
            next_page_is_enable=self.driver._is_element_present("Xpath=//a[text()='Next']")
            #print 'Next page exist status:'+str(next_page_is_enable)
            if self.driver._is_element_present("Xpath=//a[text()='"+playName+"']"):
                num_this_page=0
                is_recommendationExist = True
                num_this_page=int(self.driver.get_matching_xpath_count("//a[text()='"+playName+"']"))
                num_recommendation=int(num_recommendation)+int(num_this_page)
                log.info('new play exist in this page and num is %s' % (str(num_this_page)))
                log.info('new play exist total num so far is %s' % (str(num_recommendation)))
            if next_page_is_enable:
                log.info('Next page exist then click it to next page')
                self.driver.wait_until_page_contains_element("Xpath=//a[text()='Next']",self.timeout)
                self.driver.click_link("Xpath=//a[text()='Next']")
                num_page=num_page+1
                #print '======='
                self.driver.wait_until_page_contains_element("Xpath=//span[text()='Page']//input[@value='"+str(num_page)+"']",self.timeout)
                self.driver.wait_until_page_contains_element('Xpath=//div[@id="ext-gen11"]/div',self.timeout)
                #print '-------'
            else:
                log.info('This is the last page!')
                next_page_is_enable=False
            #print next_page_is_enable
        try:
            assert is_recommendationExist
            log.info("All Recommendations exist")
        except AssertionError:
            log.error("Sync Data Failed")
        return num_recommendation
    def resetSFDC(self):
        try:
            self.driver.go_to(SalePrismEnvironments.resetUrl)
            self.driver.wait_until_page_contains_element("Xpath=//input[@value='Delete all Lattice Data (except error logs or setup data) - May need to be run multiple times']",self.timeout)
            self.driver.click_button("Xpath=//input[@value='Delete all Lattice Data (except error logs or setup data) - May need to be run multiple times']")
            time.sleep(5)
            self.driver.wait_until_page_contains_element("//input[@value='Reset Batch Chain Settings Timestamps']",self.timeout)
            self.driver.click_button("//input[@value='Reset Batch Chain Settings Timestamps']")
        except Exception,e:
            log.error("reset SFDC failed Error is: %s"%e.message)
        else:
            log.info("reset SFDC successed")
    def CheckBSAndTPInRecommendation(self,playName):
        TP_Result_List=[]
        BS_Result_List=[]
        log.info('Go to Lattice Recommondation page')
        self.driver.wait_until_page_contains_element("Xpath=//a[text()='Lattice Recommendations']",self.timeout)
        self.driver.click_link("Xpath=//a[text()='Lattice Recommendations']")
        self.driver.wait_until_page_contains_element('Xpath=//div[@id="ext-gen11"]/div',self.timeout)
        log.info('###This need the certificatino for iframe have been finished in QA environment###')
        self.driver.wait_until_page_contains_element("Xpath=//a[text()='"+playName+"']",self.timeout)
        self.driver.click_link("Xpath=//a[text()='"+playName+"']")
        log.info('Go To recommendation dante detail page')
        self.driver.wait_until_page_contains_element("Xpath=//a[span/text()='Talking Points']",self.timeout)
        self.driver.click_link("Xpath=//a[span/text()='Talking Points']")
        log.info('Go To Talking points tab')
        time.sleep(5)
        self.driver.select_frame('theIFrame')
        try:
            self.driver.wait_until_page_contains("No Recommendations Found",self.timeout)
            log.info('No data be found')
        except Exception,e:
            log.info(e)
            TP_Result_List=self.sf.GetTalkingPoints()
        self.driver.unselect_frame()
        self.driver.wait_until_page_contains_element("Xpath=//a[span/text()='Buying Signals']",self.timeout)
        self.driver.click_link("Xpath=//a[span/text()='Buying Signals']")
        log.info('Go To Buying Signals tab')
        time.sleep(5)
        self.driver.select_frame('theIFrame')
        try:
            self.driver.wait_until_page_contains("No Recommendations Found",self.timeout)
            log.info('No data be found')
        except Exception,e:
            log.info(e)
            BS_Result_List=self.sf.GetBuyingSignalsInfo()()
        self.driver.unselect_frame()
        log.info('completed')
        #self.sf.GetTalkingPoints()
    def quit(self):
        self.sf.Close_browser()

#if __name__ == '__main__':
    #des=DealSFDC1()
    #des.configDanteServer()