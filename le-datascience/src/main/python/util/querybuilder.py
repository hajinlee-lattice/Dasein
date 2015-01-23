import pyodbc

class QueryBuilder(object):
    '''
      Take a look at http://www.tryolabs.com/Blog/2012/06/25/connecting-sql-server-database-python-under-ubuntu/
      if trying to run this under Ubuntu Linux
    '''
    
    def __init__(self, dsn, username, password, database):
        conn_string = "DSN=%s;UID=%s;PWD=%s;DATABASE=%s;" % (dsn, username, password, database)
        self.conn = pyodbc.connect(conn_string)
       
    def executeQuery(self, query):
        cursor = self.conn.cursor()
        return cursor.execute(query)