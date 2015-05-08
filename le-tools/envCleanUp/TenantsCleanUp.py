#!/usr/local/bin/python
# coding: utf-8


'''
Created on 2015��5��8��

@author: GLiu
'''
from ConfigParser import SafeConfigParser
import pypyodbc as pyodbc
import traceback
from subprocess import PIPE, Popen
import os.path


def CleanUp(): 
    tenant = tenants();
    '''
    Delete the tenants which created date is more than days_keep days and in days_using days has not been used. keep the tenants which in tenants_keep list
    '''
    del_tenants = tenant.getDeleteTenantsViaDateTime()
    
    if len(del_tenants)>0:
        for del_tenant in del_tenants:
            if env.tenants_keep.find(del_tenant.get("name"))==-1:
                tenant.delTenant(del_tenant.get("name"),del_tenant.get("visidbconnectionstring"))
    else:
        print "there is no tenants should be removed via datetime filter conditions!"
      
    '''
     Delete the tenants which are in the tenants_delete list.
    '''   
    del_tenants = tenant.getDeleteTenantsViaDeleteList()
    if len(del_tenants)>0:
        for del_tenant in del_tenants:
            tenant.delTenant(del_tenant.get("name"),del_tenant.get("visidbconnectionstring"))
    else:
        print "there is no tenants should be removed in delete list!"
    
def InitialClear(): 
    tenant = tenants();
    
    del_tenants = tenant.getInitialDeleteTenants()
    
    if len(del_tenants)>0:
        for del_tenant in del_tenants:
            if env.tenants_keep.find(del_tenant.get("name"))==-1:
                tenant.delTenant(del_tenant.get("name"),del_tenant.get("visidbconnectionstring"))
    else:
        print "there is no tenants should be removed!"

def unitTest(): 
    print env.tenants_keep
    print len(env.tenants_keep.split(","))    
        
class tenants(object):
    
    def delTenant(self,tenant_name,visidb_conn):
        print "##################:deleting the tenant: %s" % tenant_name
        if self.delFromVisidb(tenant_name, visidb_conn):
            return self.delFromDataLoader(tenant_name)
        else:
            print "==========>Failed to delete the visidb: %s from visidb server: %s" % (tenant_name,visidb_conn)
            return False;
    def delFromVisidb(self,tenant_name,visidb_conn):
        dlc_command = "%s\dlc -DD -s %s -u %s -p %s -dc \"%s\" -dn %s" % (env.dl_dlc_path,env.dl_server,env.dl_server_user,env.dl_server_pwd,visidb_conn,tenant_name)
#         print dlc_command
        return self.runCommand(dlc_command)
        
    def delFromDataLoader(self,tenant_name):
        dlc_command = "%s\dlc -DT -s %s -u %s -p %s -t %s" % (env.dl_dlc_path,env.dl_server,env.dl_server_user,env.dl_server_pwd,tenant_name)
#         print dlc_command
        return self.runCommand(dlc_command)
        
    def getDeleteTenants(self):
        del_tenants = self.getDeleteTenantsViaDateTime();
        if len(del_tenants)<=0:
            return self.getDeleteTenantsViaDeleteList()
        else:
            delete_list = self.getDeleteTenantsViaDeleteList()
            if len(delete_list)>0:
                del_tenants.extend(delete_list)
                
        return del_tenants
            
            
    def getDeleteTenantsViaDateTime(self):
        sql_tenants = "select name from tenant where status = 1 and name like '%s%%' and datediff(dd,CreateTime,getdate())>%s" % (env.tenants_key,env.days_keep) +\
                         " and tenantid not in (select tenantid from Launches where datediff(dd,CreateTime,getdate())<=%s)" % env.days_using
#         print sql_tenants
        names = self.getQuery(sql_tenants);
        results = [];
        for name in names:
            print "getting tenants to be removed: %s" % name[0]
            sql_tenants = "select tenantid,name,visidbconnectionString from tenant where status = 1 and name like '%%%s%%' and datediff(dd,CreateTime,getdate())>%s" % (name[0][len(env.tenants_key):-1],env.days_keep) +\
                         " and tenantid not in (select tenantid from Launches where datediff(dd,CreateTime,getdate())<=%s)" % env.days_using
#             print sql_tenants
            result = self.getQuery(sql_tenants);
            results.extend(result)
#         print sql_tenants        
        return results;
    def getDeleteTenantsViaDeleteList(self):
        sql_tenants = "select tenantid,name,visidbconnectionString from tenant where status = 1 and  name in ('%s')"  % env.tenants_delete.replace(",","','")
#         print sql_tenants
        results = self.getQuery(sql_tenants);
        return results;
    
    def getInitialDeleteTenants(self):
        sql_tenants = "select tenantid,name,visidbconnectionString from tenant where status = 2"
        results = self.getQuery(sql_tenants);
        return results;
    
    def getQuery(self, query):
        try:            
            conn = pyodbc.connect(env.SQL_conn_dataloader)
            cur = conn.cursor()
            cur.execute(query)
            results = cur.fetchall()
            cur.close()
            conn.close()
            return results
        except Exception:
            e = traceback.format_exc()
            print "\nFAILED:Could not fetch query results", e, "\n"
            return None
        
    def runCommand(self, cmd, from_dir=None):
#         print cmd
        if from_dir is None:
            from_dir = os.getcwd()
        if from_dir.startswith("~"):
            from_dir = os.path.expanduser(from_dir)
        p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True, cwd=from_dir)
        self.stdout, self.stderr = p.communicate()
        if self.stderr:            
            print self.stderr
            return False
        else:
#             print self.stdout
            return True
class env(object):
    
    # parser that read configuration properties from config.ini file
    parser = SafeConfigParser()
    parser.read('config.ini')
    
    
    #properties definition
    dl_server=parser.get('ENVInfo', 'dl_server');
    dl_server_user=parser.get('ENVInfo', 'dl_server_user'); 
    dl_server_pwd=parser.get('ENVInfo', 'dl_server_pwd'); 
    dl_dlc_path=parser.get('ENVInfo', 'dl_dlc_path'); 
    SQL_conn_dataloader=parser.get('ENVInfo', 'SQL_conn_dataloader'); 
    
    tenants_delete=parser.get('TenantInfo', 'tenants_delete');
    tenants_keep=parser.get('TenantInfo', 'tenants_keep');
    tenants_key=parser.get('TenantInfo', 'tenants_key');
    days_keep=parser.get('TenantInfo', 'days_keep');
    days_using=parser.get('TenantInfo', 'days_using');      


if __name__ == '__main__':
    CleanUp()
#     InitialClear()
#     unitTest()