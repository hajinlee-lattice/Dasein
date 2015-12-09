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
    def fetchResultOfSelect(cls,SQL,SERVER,DATABASE,UID,PWD,fetchAll=False,DRIVER=SalePrismEnvironments.ODBCSqlServer):
        conn = pyodbc.connect(DRIVER=DRIVER,SERVER=SERVER+"\\SQL2012STD",DATABASE=DATABASE,UID=UID,PWD=PWD)
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
    pass