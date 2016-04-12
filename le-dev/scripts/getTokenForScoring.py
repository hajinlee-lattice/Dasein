'''
Created on Mar 30, 2016

@author: smeng
'''

import requests
import json
import argparse
import hashlib

parser = argparse.ArgumentParser()
parser.add_argument('-env', dest='env', action='store', required=False, help='Environment: prod or qa', default='qa')
parser.add_argument('-t', dest='tenant', action='store', required=True, help='LP tenant name')
parser.add_argument('-u', dest='username', action='store', required=False, help='username for LP', default='bnguyen@lattice-engines.com')
parser.add_argument('-p', dest='password', action='store', required=False, help='password for LP', default='tahoe')
args = parser.parse_args()



LP_SERVER_HOST = ''
if (args.env == 'prod'):
    LP_SERVER_HOST = 'https://app3.lattice-engines.com'
elif (args.env == 'qa'):
    LP_SERVER_HOST = 'http://app.lattice.local'


def getToken():
    auth_token = __getAuthenticationToken(args.tenant, args.username, hashlib.sha256(args.password).hexdigest())
    access_token = __getAccessToken(args.tenant, auth_token)
    print '\n**************************************************'
    print 'ACCESS TOKEN: ' + access_token
    print '**************************************************\n'



def __getAuthenticationToken(tenant, username, password):
    # print 'Password: ' + password
    url_login = LP_SERVER_HOST + "/pls/login"
    body_login = {"Username": username, "Password": password}
    header_login = {"Content-Type": "application/json"}
    response = requests.post(url_login, headers=header_login, data=json.dumps(body_login))
    assert response.status_code == 200, 'Request failed for logging in to LP.\n' + response.content
    results = json.loads(response.text)
    auth_token = results["Uniqueness"] + '.' + results["Randomness"]

    url_attach = LP_SERVER_HOST + '/pls/attach'
    body_attach = {"Identifier": '%s.%s.Production' % (tenant, tenant), "DisplayName": tenant}
    header_attach = {"Content-Type": "application/json", "Authorization": auth_token}
    response_attach = requests.post(url_attach, headers=header_attach, data=json.dumps(body_attach))

    assert response_attach.status_code == 200, 'Request failed for logging in to LP.\n' + response_attach.content
    # print 'got the auth token: ' + auth_token
    return auth_token

def __getAccessToken(tenant, auth_token):
    url_oauth = LP_SERVER_HOST + '/pls/oauth2/accesstoken?tenantId=%s.%s.Production' % (tenant, tenant)
    header = {"Authorization": auth_token}
    response = requests.get(url_oauth, headers=header)
    assert response.status_code == 200, 'Failed to get access Token.\n' + response.content

    return response.text


if __name__ == '__main__':
    getToken()
    pass
