__author__ = 'BWang'
from PlaymakerEnd2End.Configuration.Properties import SalePrismEnvironments
try:
    import  pyodbc
except  ImportError:
    import os
    os.system('pip install -U pyodbc')
    import pyodbc
class DealDB(object):
    @classmethod
    def fetchResultOfSelect(cls,SQL,fetchAll=False,DRIVER='{SQL SERVER}',SERVER=SalePrismEnvironments.host,DATABASE=SalePrismEnvironments.tenantName,UID=SalePrismEnvironments.DBUser,PWD=SalePrismEnvironments.DBPwd):
        conn = pyodbc.connect(DRIVER=DRIVER,SERVER=SERVER,DATABASE=DATABASE,UID=UID,PWD=PWD)
        cur = conn.cursor()
        assert cur != None
        cur.execute(SQL)
        if fetchAll:
            result=cur.fetchall()
        else:
            result=cur.fetchone()
        conn.close()
        return result


if __name__ == "__main__":
    sql="SELECT LaunchId,CreateTime FROM Launches where tenantid=(SELECT TenantId FROM Tenant where name='20151125205352345') and GroupName='Full_Dante_Data_Flow' "
    a= DealDB.fetchResultOfSelect(SQL=sql,SERVER="10.41.1.193\SQL2012STD",DATABASE="DataLoader",fetchAll=False)
    print a[0]