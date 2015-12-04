"""
@author bwang
@createDate 11/11/2015 
""" 
import time
from PlaymakerEnd2End.Configuration.Properties import SalePrismEnvironments
try:
	from selenium import webdriver
except ImportError:
	import os
	os.system('pip install -U selenium')
	from selenium import webdriver
from selenium.webdriver.common.keys import Keys
log=SalePrismEnvironments.log
class DealSFDC(object):
	def __init__(self):
		
		if SalePrismEnvironments.driverType =="Firefox":
			self.driver=webdriver.Firefox()
			self.driver.implicitly_wait(20)
			self.driver.maximize_window()
		elif SalePrismEnvironments.driverType=="Chrome":
			pass
	def loginSF(self,user=SalePrismEnvironments.SFDCUser,pwd=SalePrismEnvironments.SFDCPWD):
		self.driver.get(SalePrismEnvironments.SFurl)
		self.driver.find_element_by_id('username').send_keys(user)
		self.driver.find_element_by_id('password').send_keys(pwd)
		self.driver.find_element_by_id('Login').click()
		time.sleep(5)
		try:
			self.driver.find_element_by_id('showMeLater').click()
		except Exception,e:
			pass
		log.info("salesforce login successfully")

	def configDanteServer(self,dante_Server=SalePrismEnvironments.dante_Server):#need run loginSF first
		if not self.islogin():
			self.loginSF()
		self.driver.get(SalePrismEnvironments.setUpPageUrl)
		time.sleep(5)
		self.driver.find_element_by_id('DevToolsIntegrate_font').click()
		self.driver.find_element_by_id('CustomSettings_font').click()
		time.sleep(3)
		self.driver.find_element_by_partial_link_text('Lattice Account iFrame Sett').click()
		self.driver.find_element_by_xpath("//input[@value='Manage']").click()
		self.driver.find_element_by_xpath("//input[@value='Edit']").click()
		self.driver.find_element_by_xpath("//label[text()='Base URL']//parent::th//parent::tr//input").clear()
		self.driver.find_element_by_xpath("//label[text()='Base URL']//parent::th//parent::tr//input").send_keys(dante_Server)
		self.driver.find_element_by_xpath("//input[@value='Save']").click()
		time.sleep(5)
		assert self.driver.find_element_by_xpath("//html").text.find(dante_Server)>0
		log.info("dante_Server set successfully")

	def configOTK(self,tenant=SalePrismEnvironments.tenantName):#need run self.loginSF first
		if not self.islogin():
			self.loginSF()
		self.driver.find_element_by_xpath("//a[text()='Lattice Admin']").click()
		try:
			time.sleep(5)
			if self.driver.find_element_by_xpath("//h3[text()='You are authenticated']").is_displayed():
				self.driver.find_element_by_xpath("//a[text()='Disconnect']").click()
				self.driver.find_element_by_xpath("//input[@value='Disconnect']").click()
		except Exception,e:
			pass
		time.sleep(5)
		self.driver.find_element_by_xpath("//a[text()='Authenticate Now']").click()
		self.driver.find_element_by_xpath("//label[text()='Customer ID:']//parent::div//child::input").clear()
		self.driver.find_element_by_xpath("//label[text()='Customer ID:']//parent::div//child::input").send_keys(tenant)
		self.driver.find_element_by_xpath("//label[text()='Token:']//parent::div//child::input").clear()
		print SalePrismEnvironments.OTK
		self.driver.find_element_by_xpath("//label[text()='Token:']//parent::div//child::input").send_keys(SalePrismEnvironments.OTK)
		time.sleep(5)
		self.driver.find_element_by_xpath("//input[@value='Connect']").click()
		time.sleep(15)
		assert self.driver.find_element_by_xpath("//h3[text()='You are authenticated']").is_displayed()
		log.info("Authentication done!")
	def syncData(self):#need run self.loginSF first.
		if not self.islogin():
			self.loginSF()
		self.driver.find_element_by_xpath("//a[text()='Lattice Admin']").click()
		assert self.driver.find_element_by_xpath("//h3[text()='You are authenticated']").is_displayed()
		time.sleep(5)
		self.driver.find_element_by_partial_link_text("Sync Data Now").click()
		time.sleep(6)
		self.driver.find_element_by_xpath("//input[@value='Sync']").click()
		time.sleep(5)
		assert self.driver.find_element_by_xpath("//html").text.find("The Sync Data job was initiated successfully")>0
		time.sleep(20)#wait for sync data process finish

	def islogin(self):
		try:
			self.driver.find_element_by_xpath("//span[@id='userNavLabel']")
		except Exception,e:
			return False
		else:
			return True
	def resetSFDC(self):
		try:
			self.driver.get(SalePrismEnvironments.resetUrl)
			clearDataButton=self.driver.find_element_by_xpath("//input[@value='Delete all Lattice Data (except error logs or setup data) - May need to be run multiple times']")
			clearDataButton.click()
			time.sleep(5)
			resetTimeStampButton=self.driver.find_element_by_xpath("//input[@value='Reset Batch Chain Settings Timestamps']")
			resetTimeStampButton.click()
		except Exception,e:
			log.error("reset SFDC failed Error is: %s"%e.message)
		else:
			log.info("reset SFDC successed")
	def checkRecommendations(self,playName):
		if not self.islogin():
			self.loginSF()
		self.driver.find_element_by_xpath("//a[text()='Lattice Recommendations']").click()
		is_recommendationExist = False
		pageNumber=int(self.driver.find_element_by_xpath("//span[text()='Page']").text[-1])
		for i in range(1,pageNumber+1):
			self.driver.find_element_by_xpath("//span[text()='Page']//input").clear()
			self.driver.find_element_by_xpath("//span[text()='Page']//input").send_keys(i)
			self.driver.find_element_by_xpath("//span[text()='Page']//input").send_keys(Keys.ENTER)
			time.sleep(5)
			if self.driver.find_element_by_xpath("//a[text()='"+playName+"']").is_displayed():
				is_recommendationExist = True
				break
		try:
			assert is_recommendationExist
		except AssertionError:
			log.error("Sync Data Failed")
		else:
			log.info("Sync Data successfully")
	def checkAccountPage(self,playName):
		if not self.islogin():
			self.loginSF()

def main():
	d=DealSFDC()
	d.loginSF()
	print d.islogin()
	d.configDanteServer()
	d.configOTK()
	d.resetSFDC()
	#d.syncData()
if __name__ == '__main__':
	main()