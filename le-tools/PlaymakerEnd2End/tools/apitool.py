'''
Created on Aug 7, 2015

@author: smeng
'''

import requests
import json
import argparse
import logging

 
apiUrl = 'http://testapi.lattice-engines.com:8080/playmaker'
tenantUrl = "http://testapi.lattice-engines.com:8080/tenants"
oauthUrl = "http://testoauth.lattice-engines.com:8080/oauth/token"   

def getOneTimeKey(tenant, jdbcUrl):
    request = requests.post(tenantUrl,
                      json={"TenantName":tenant,"TenantPassword":"null","ExternalId":tenant,"JdbcDriver":"com.microsoft.sqlserver.jdbc.SQLServerDriver",
                            "JdbcUrl":jdbcUrl,"JdbcUserName":"playmaker","JdbcPassword":"playmaker"})
    assert request.status_code == 200
    response = json.loads(request.text)
    assert response['TenantPassword'] != None
    return response['TenantPassword']


def getToken(key, tenant):
    params = {'grant_type':'password', 'username':tenant, 'password':key}
    headers = {'Authorization':'Basic cGxheW1ha2VyOg=='}
    request = requests.post(oauthUrl, params=params, headers=headers)
    assert request.status_code == 200
    assert json.loads(request.text)['access_token'] != None
    return json.loads(request.text)['access_token']



def getKey(tenant, jdbcUrl):
    key = getOneTimeKey(tenant, jdbcUrl)
    print 'key is: ' + key

### 'jdbc:sqlserver://10.41.1.82\\SQL2012std;databaseName=ADEDTBDd720154nN280153n154'


def getRecCount():
    url = apiUrl + "/recommendationcount"
    startTime = 0
    
    tenant = 'TestGetRecCount'
    key = getOneTimeKey(tenant, 'jdbc:sqlserver://10.41.1.83\\SQL2012std;databaseName=ADEDTBDd720154nW280139n154')
    testToken = 'bearer ' + getToken(key, tenant)
    
    headers = {'Authorization':testToken}
    params = {'start':startTime, 'destination':'SFDC'}
    request = requests.get(url, headers=headers, params=params)
    print request.text
    
def getRecommendations():
    url = apiUrl + "/recommendations"
    startTime = 0
    
    tenant = 'TestGetRec'
    key = getOneTimeKey(tenant, 'jdbc:sqlserver://10.41.1.83\\SQL2012std;databaseName=ADEDTBDd720154nW280139n154')
    testToken = 'bearer ' + getToken(key, tenant)
    
    headers = {'Authorization':testToken}
    params = {'start':startTime, 'offset':'0', 'maximum':'1000','destination':'MAP'}
    request = requests.get(url, headers=headers, params=params)
    print request.text

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-func', '--function', dest = 'function_name', action = 'store', required = True, help = 'name of the function')
    parser.add_argument('-t', '--tenant', dest = 'tenant', action = 'store', required = True, help = 'name of the tenant')
    parser.add_argument('-db', '--database', dest = 'database', action = 'store', required = True, help = 'database connection string')
    args = parser.parse_args()
    
    if args.function_name == 'getKey':
        getKey(args.tenant, args.database)
    else:
        logging.error('No such function: ' + args.function_name)

if __name__ == '__main__':
    main()