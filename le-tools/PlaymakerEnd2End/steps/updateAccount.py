"""
@author bwang
@createDate 11/11/2015 
""" 
try:
	import pyodbc
except Exception,e:
	import os
	os.system('pip install -U pyodbc')
	import pyodbc
import json
from PlaymakerEnd2End.Configuration.Properties import SalePrismEnvironments
log=SalePrismEnvironments.log
def updateTenantAccount(tenant=SalePrismEnvironments.tenantName):
	log.info("##########  Account Match process starts   ##########")
	with open(SalePrismEnvironments.rootPath+'AccountJson.json') as jsonData:
		data=json.load(jsonData)
	conn = pyodbc.connect(DRIVER=SalePrismEnvironments.ODBCSqlServer,SERVER=SalePrismEnvironments.tenantDBUrl,DATABASE=tenant,UID=SalePrismEnvironments.tenantDBUser,PWD=SalePrismEnvironments.tenantDBPassword)
	cur = conn.cursor()
	assert cur!=None
	keys=data.keys()
	try:
		for k in keys:
			value=data.get(k)
			updateSQL="update ["+tenant+"].[dbo].[LEAccount] set CrmAccount_External_ID='"+value+"' ,Alt_ID='"+value+"'  where ["+tenant+"].[dbo].[LEAccount].[External_ID] = '"+k+"'"
			cur.execute(updateSQL)
			conn.commit()
	except Exception,e:
		log.error(e)
	else:
		print "tenant DB account match successfully"
	finally:
		conn.close()

def main():
	updateTenantAccount()
if __name__ == '__main__':
	main()