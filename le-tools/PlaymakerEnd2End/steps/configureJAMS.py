"""
@author bwang
@createDate 11/11/2015 
""" 
try:
	import pyodbc
except ImportError:
	import os
	assert os.system('pip install -U pyodbc') ==0
	import pyodbc
from PlaymakerEnd2End.Configuration.Properties import SalePrismEnvironments
log=SalePrismEnvironments.logProvider.getLog("configureJams",True)
def update247DB(tenant=SalePrismEnvironments.tenantName,qname=SalePrismEnvironments.QueneName):
	log.info("########## update JAMSCFG table on 247 DB configuration start   ##########")
	updateQName="update Tenants set Queue_Name='"+qname+"' where Tenant = '"+tenant+"'"
	updateDQName = "update Tenants set Dante_Queue_Name='"+qname+"' where Tenant = '"+tenant+"'"
	conn=None
	try:
		conn = pyodbc.connect(DRIVER='{SQL SERVER}',SERVER='10.41.1.247',DATABASE='JAMSCFG',UID=SalePrismEnvironments.DBUser,PWD=SalePrismEnvironments.DBPwd)
		cur = conn.cursor()
		assert cur != None
		cur.execute(updateQName)
		conn.commit()
		cur.execute(updateDQName)
		conn.commit()
	except Exception,e:
		log.error(e)
	else:
		log.info("update 247 JAMS DB successfully")
	finally:
		if conn!=None:
			conn.close()

def main():
	update247DB()
if __name__ == '__main__':
	main()