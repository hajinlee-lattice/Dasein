'''
Created on May 6, 2014

@author: hliu
'''

class DBCreds(object):
    '''
    classdocs
    '''
    
    def __init__(self, host, port, db_name, user, passwd):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.user = user
        self.passwd = passwd
