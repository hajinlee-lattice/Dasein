'''
Created on 11/12/2015
@author: bwang
'''
import json,os,time
from PlaymakerEnd2End.tools.LogTool import LogFactory
from PlaymakerEnd2End.tools.apitool import getOneTimeKey
try:
	from selenium import webdriver
except Exception,e:
	import os
	os.system('pip install -U selenium')
	from selenium import webdriver
class SalePrismEnvironments(object):
    "parser that read configuration properties from config.ini file"
    with open("..\\config.ini") as configFile:
        paras = json.load(configFile)
    # properties definition in config.ini
    withModelingOnDataPlatform=paras.get("withModelingOnDataPlatform")
    tenantName=paras.get('tenantName')
    host=paras.get('host')
    QueneName=paras.get('QueneName')
    playName=paras.get('playName')
    playType=paras.get("playType")
    SFurl=paras.get("SFurl")
    setUpPageUrl=paras.get("setUpPage")
    SFDCUser=paras.get('SFDCUser')
    SFDCPWD=paras.get('SFDCPWD')
    SPUser=paras.get("SPUser")
    resetUrl=paras.get("resetUrl")
    SPPwd=paras.get("SPPwd")
    tenantUrl=paras.get("tenantUrl")
    dante_Server='https://'+host+'/DT_'+tenantName
    jdbc='jdbc:sqlserver://'+host+'\\SQL2012STD;databaseName='+tenantName
    sPrismUrl='https://'+host+'/'+tenantName+'_application'
    dataloaderUpdateRESTURL=paras.get("dataloaderUrl")+"/DLRestService/UpdateDataProvider"
    dataloaderGetLaunchStatusURL=paras.get("dataloaderUrl")+"/DLRestService/GetLaunchStatus"
    dataloaderUrl=paras.get("dataloaderUrl")
    AuthorizationStr=paras.get("AuthorizationStr")
    validatePostXML=paras.get("validatePostXML")
    resetCachePostXML=paras.get("resetCachePostXML")
    savePlayUrl="https://"+host+"/"+tenantName+"_Application/WebPlayServiceHost.svc/SavePlayDetailsReturnStatus"
    scorePlayUrl="https://"+host+"/"+tenantName+"_Application/WebPlayServiceHost.svc/RequestCombinedModelScore?playID="
    approvePlayUrl="https://"+host+"/"+tenantName+"_Application/WebPlayServiceHost.svc/MarkApproved?playID=99&approve=true"
    getStatusOfPlayUrl="https://"+host+"/"+tenantName+"_Application/WebPlayServiceHost.svc/GetPlayStatus?playID="
    getLaunchStatusUrl="https://"+host+"/"+tenantName+"_Application/WebPlayServiceHost.svc/GetPlayDetailsWithHeader?playID="
    getPortfililPlaysUrl="https://"+host+"/"+tenantName+"_Application/WebPlayServiceHost.svc/GetPortfolioPlays?queryName=AllPortfolioPlays"
    launchPlaysUrl="https://"+host+"/"+tenantName+"_Application/WebPlayServiceHost.svc/LaunchPlays?simulate=false&queryName=AllPortfolioPlays"
    dataloaderDBUrl=paras.get("dataloaderDBUrl")
    dataloaderDBUser=paras.get("dataloaderDBUser")
    dataloaderDBPassword=paras.get("dataloaderDBPassword")
    dataloaderDBName=paras.get("dataloaderDBName")
    JAMSDBUrl=paras.get("JAMSDBUrl")
    JAMSDBName=paras.get("JAMSDBName")
    JAMSDBUser=paras.get("JAMSDBUser")
    JAMSDBPassword=paras.get("JAMSDBPassword")
    tenantDBUser=paras.get("tenantDBUser")
    tenantDBPassword=paras.get("tenantDBPassword")
    #configuration in Properties
    log=LogFactory.getLog("End2End",True)
    OTK=getOneTimeKey(tenantName,jdbc)
    ODBCSqlServer="{SQL SERVER}"
    tenantDBUrl=host
    #webdriver
    driverType=paras.get("driverType")
    def __init__(self):
        pass

def main():
    s=SalePrismEnvironments()

if __name__ == '__main__':
    main()
