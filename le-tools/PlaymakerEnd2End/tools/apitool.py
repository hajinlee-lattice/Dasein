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
                      json={"TenantName":tenant, "TenantPassword":"null", "ExternalId":tenant, "JdbcDriver":"com.microsoft.sqlserver.jdbc.SQLServerDriver",
                            "JdbcUrl":jdbcUrl, "JdbcUserName":"playmaker", "JdbcPassword":"playmaker"})
    assert request.status_code == 200
    response = json.loads(request.text)
    assert response['TenantPassword'] != None
    return response['TenantPassword']


def getAccessToken(key, tenant):
    response = getTokenJsonFromKey(key, tenant)
    assert json.loads(response)['access_token'] != None
    return json.loads(response)['access_token']

def getTokenJsonFromKey(key, tenant):
    params = {'grant_type':'password', 'username':tenant, 'password':key}
    headers = {'Authorization':'Basic cGxheW1ha2VyOg=='}
    request = requests.post(oauthUrl, params=params, headers=headers)
    assert request.status_code == 200
    return request.text

def getKey(tenant, jdbcUrl):
    key = getOneTimeKey(tenant, jdbcUrl)
    print 'key is: ' + key

def getTokenJson(tenant, jdbcUrl):
    key = getOneTimeKey(tenant, jdbcUrl)
    print 'Access token is: ' + getTokenJsonFromKey(key, tenant)



def getRecordCount(response):
    return len(json.loads(response)['records'])

def getStartTimestamp(response):
    return int(json.loads(response)['startDatetime'])

def getEndTimestamp(response):
    return int(json.loads(response)['endDatetime'])


def getData(tenant, jdbcUrl):
    url = apiUrl + "/recommendations"
    startTime = 0

    key = getOneTimeKey(tenant, jdbcUrl)
    testToken = 'bearer ' + getAccessToken(key, tenant)

    headers = {'Authorization':testToken}
    params = {'start':startTime, 'offset':'0', 'maximum':'1000', 'destination':'SFDC'}
    request = requests.get(url, headers=headers, params=params)
    print request.text

def getDataFromKey(tenant, key):
    url = apiUrl + "/recommendations"
    startTime = 0

    testToken = 'bearer ' + getAccessToken(key, tenant)

    headers = {'Authorization':testToken}
    params = {'start':startTime, 'offset':'0', 'maximum':'1000', 'destination':'SFDC'}
    request = requests.get(url, headers=headers, params=params)
    print request.text


def getDataFromToken(token):
    url = apiUrl + "/recommendations"
    startTime = 0

    token = 'bearer ' + token
    headers = {'Authorization':token}
    params = {'start':startTime, 'offset':'0', 'maximum':'1000', 'destination':'SFDC'}
    request = requests.get(url, headers=headers, params=params)
    print request.text


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command')

    # getKey
    parser_getKey = subparsers.add_parser('getKey', help='get one time key')
    parser_getKey.add_argument('-db', '--Database', dest='database', action='store', required=True, help='database connection string')
    parser_getKey.add_argument('-t', '--Tenant', dest='tenant', action='store', required=True, help='tenant id')

    # getToken
    parser_getToken = subparsers.add_parser('getToken', help='get token json')
    parser_getToken.add_argument('-db', '--Database', dest='database', action='store', required=True, help='database connection string')
    parser_getToken.add_argument('-t', '--Tenant', dest='tenant', action='store', required=True, help='tenant id')

    # getData
    parser_getData = subparsers.add_parser('getData', help='get data')
    parser_getData.add_argument('-db', '--Database', dest='database', action='store', required=True, help='database connection string')
    parser_getData.add_argument('-t', '--Tenant', dest='tenant', action='store', required=True, help='tenant id')

    # getDataFromKey
    parser_getDataFromKey = subparsers.add_parser('getDataFromKey', help='get data from one time key')
    parser_getDataFromKey.add_argument('-k', '--Key', dest='key', action='store', required=True, help='one time key')
    parser_getDataFromKey.add_argument('-t', '--Tenant', dest='tenant', action='store', required=True, help='tenant id')

    # getDataFromToken
    parser_getDataFromToken = subparsers.add_parser('getDataFromToken', help='data from token')
    parser_getDataFromToken.add_argument('-tk', '--Token', dest='token', action='store', required=True, help='access token')

    args = parser.parse_args()

    if args.command == 'getKey':
        getKey(args.tenant, args.database)
    elif args.command == 'getToken':
        getTokenJson(args.tenant, args.database)
    elif args.command == 'getData':
        getData(args.tenant, args.database)
    elif args.command == 'getDataFromKey':
        getDataFromKey(args.tenant, args.key)
    elif args.command == 'getDataFromToken':
        getDataFromToken(args.token)
    else:
        logging.error('No such function: ' + args.function_name)

if __name__ == '__main__':
    main()
