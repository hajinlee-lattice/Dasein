"""
@author bwang
@createDate 11/11/2015 
""" 
import json,sys,os
try:
	import requests
except Exception,e:
	assert os.system('pip install requests') ==0
	import requests
requests.packages.urllib3.disable_warnings()
sys.path.append("..")
from Configuration.Properties import SalePrismEnvironments

def getOneTimeKey(tenant,jdbcUrl):
	request = requests.post(SalePrismEnvironments.tenantUrl,
					  json={"TenantName":tenant,"TenantPassword":"null","ExternalId":tenant,"JdbcDriver":"com.microsoft.sqlserver.jdbc.SQLServerDriver",
							"JdbcUrl":jdbcUrl,"JdbcUserName":"playmaker","JdbcPassword":"playmaker"})
	assert request.status_code == 200
	response = json.loads(request.text)
	assert response['TenantPassword'] != None
	return response['TenantPassword']
def getKey(tenant=SalePrismEnvironments.tenantName, jdbcUrl=SalePrismEnvironments.jdbc):
	key = getOneTimeKey(tenant,jdbcUrl)
	return key

def main():
	print getKey()
		
if __name__=="__main__":
	main()
