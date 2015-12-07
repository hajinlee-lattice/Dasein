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
    def fetchAllResultOfSelect(cls,SQL,DRIVER='{SQL SERVER}',SERVER=SalePrismEnvironments.host,DATABASE=SalePrismEnvironments.tenantName,UID=SalePrismEnvironments.DBUser,PWD=SalePrismEnvironments.DBPwd):
        conn = pyodbc.connect(DRIVER=DRIVER,SERVER=SERVER,DATABASE=DATABASE,UID=UID,PWD=PWD)
        cur = conn.cursor()
        assert cur != None
        cur.execute(SQL)
        result=cur.fetchall()
        conn.close()
        return result

if __name__ == "__main__":
    print DealDB.fetchAllResultOfSelect(SQL="SELECT  count(*)  FROM PreLead where Status=2800 and Play_ID=45")