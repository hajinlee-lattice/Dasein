'''
Created on Dec 29, 2015

@author: smeng
'''

import  pyodbc

class SqlServerClient():

    def __init__(self, server, database, user, password):
        driver = "{SQL SERVER}"
        self.conn = pyodbc.connect(DRIVER=driver, SERVER=server, DATABASE=database, UID=user, PWD=password)


    def queryAll(self, SQL):
        cur = self.conn.cursor()
        assert cur != None
        cur.execute(SQL)
        result = cur.fetchall()
        return result

    def queryOne(self, SQL, fetchAll=False):
        cur = self.conn.cursor()
        assert cur != None
        cur.execute(SQL)
        result = cur.fetchone()
        return result

    def disconnect(self):
        self.conn.close()

    def commitUpdate(self, SQL):
        cur = self.conn.cursor()
        assert cur != None
        isSuccessful = False
        try:
            cur.execute(SQL)
            self.conn.commit()
            isSuccessful = True
        except Exception, e:
            print e.message
        return isSuccessful



if __name__ == "__main__":
    pass
    '''
    client = SqlServerClient(server='10.41.1.193\SQL2008R2', database='TestAPI_DB_75', user='playmaker', password='playmaker')
    res = client.queryAll("select top 5 * from play")
    client.disconnect()
    print res[4].Display_Name
    '''