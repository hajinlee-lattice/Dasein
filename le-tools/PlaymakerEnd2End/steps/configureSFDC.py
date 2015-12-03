"""
@author bwang
@createDate 11/11/2015 
""" 
import time,sys
sys.path.append("..")
from Configuration.Properties import SalePrismEnvironments
try:
	from selenium import webdriver
except Exception,e:
	import os
	os.system('pip install -U selenium')
	from selenium import webdriver
from selenium.webdriver.common.keys import Keys
log=SalePrismEnvironments.log

def loginSF(ff=SalePrismEnvironments.ff,user=SalePrismEnvironments.SFDCUser,pwd=SalePrismEnvironments.SFDCPWD):
	SFurl='https://login.salesforce.com/'
	ff.get(SFurl)
	ff.find_element_by_id('username').send_keys(user)
	ff.find_element_by_id('password').send_keys(pwd)
	ff.find_element_by_id('Login').click()
	time.sleep(5)
	try:
		ff.find_element_by_id('showMeLater').click()
	except Exception,e:
		pass
	log.info("salesforce login successfully")

def configDanteServer(ff=SalePrismEnvironments.ff,dante_Server=SalePrismEnvironments.dante_Server):#need run loginSF first
	ff.get("https://na34.salesforce.com/setup/forcecomHomepage.apexp")
	time.sleep(5)
	ff.find_element_by_id('DevToolsIntegrate_font').click()
	ff.find_element_by_id('CustomSettings_font').click()
	time.sleep(3)
	ff.find_element_by_partial_link_text('Lattice Account iFrame Sett').click()
	ff.find_element_by_xpath("//input[@value='Manage']").click()
	ff.find_element_by_xpath("//input[@value='Edit']").click()
	ff.find_element_by_xpath("//label[text()='Base URL']//parent::th//parent::tr//input").clear()
	ff.find_element_by_xpath("//label[text()='Base URL']//parent::th//parent::tr//input").send_keys(dante_Server)
	ff.find_element_by_xpath("//input[@value='Save']").click()
	time.sleep(5)
	assert ff.find_element_by_xpath("//html").text.find(dante_Server)>0
	log.info("dante_Server set successfully")

def configOTK(ff=SalePrismEnvironments.ff,tenant=SalePrismEnvironments.tenantName):#need run loginSF first

	ff.find_element_by_xpath("//a[text()='Lattice Admin']").click()
	try:
		time.sleep(5)
		if ff.find_element_by_xpath("//h3[text()='You are authenticated']").is_displayed():
			ff.find_element_by_xpath("//a[text()='Disconnect']").click()
			ff.find_element_by_xpath("//input[@value='Disconnect']").click()
	except Exception,e:
		pass
	time.sleep(5)
	ff.find_element_by_xpath("//a[text()='Authenticate Now']").click()
	ff.find_element_by_xpath("//label[text()='Customer ID:']//parent::div//child::input").clear()
	ff.find_element_by_xpath("//label[text()='Customer ID:']//parent::div//child::input").send_keys(tenant)
	ff.find_element_by_xpath("//label[text()='Token:']//parent::div//child::input").clear()
	print SalePrismEnvironments.OTK
	ff.find_element_by_xpath("//label[text()='Token:']//parent::div//child::input").send_keys(SalePrismEnvironments.OTK)
	time.sleep(5)
	ff.find_element_by_xpath("//input[@value='Connect']").click()
	time.sleep(15)
	assert ff.find_element_by_xpath("//h3[text()='You are authenticated']").is_displayed()
	log.info("Authentication done!")
def syncData(ff=SalePrismEnvironments.ff):#need run loginSF first.
	ff.find_element_by_xpath("//a[text()='Lattice Admin']").click()
	assert ff.find_element_by_xpath("//h3[text()='You are authenticated']").is_displayed()
	time.sleep(5)
	ff.find_element_by_partial_link_text("Sync Data Now").click()
	time.sleep(6)
	ff.find_element_by_xpath("//input[@value='Sync']").click()
	time.sleep(5)
	assert ff.find_element_by_xpath("//html").text.find("The Sync Data job was initiated successfully")>0
	time.sleep(20)
	ff.find_element_by_xpath("//a[text()='Lattice Recommendations']").click()
	is_recommendationExist = False
	pageNumber=int(ff.find_element_by_xpath("//span[text()='Page']").text[-1])
	for i in range(1,pageNumber+1):
		ff.find_element_by_xpath("//span[text()='Page']//input").clear()
		ff.find_element_by_xpath("//span[text()='Page']//input").send_keys(i)
		ff.find_element_by_xpath("//span[text()='Page']//input").send_keys(Keys.ENTER)
		time.sleep(5)
		if ff.find_element_by_xpath("//a[text()='"+SalePrismEnvironments.playName+"']").is_displayed():
			is_recommendationExist = True
			break
	try:
		assert is_recommendationExist
	except AssertionError:
		log.error("Sync Data Failed")
	else:
		log.info("Sync Data successfully")
def main():
	loginSF()
	configDanteServer()
	configOTK()
	syncData()
if __name__ == '__main__':
	main()