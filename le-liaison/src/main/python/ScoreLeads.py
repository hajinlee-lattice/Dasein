#!/usr/bin/python

import sys, requests, codecs, csv, json
from datetime import datetime
import time
import multiprocessing
import subprocess
import hashlib

#PLS_ENDPOINT = 'https://app3.lattice-engines.com/pls'
#SCORING_ENDPOINT = 'https://api3.lattice-engines.com/score'
PLS_ENDPOINT = 'https://bodcprodsvipa111.prod.lattice.local:8081/pls'
SCORING_ENDPOINT = 'https://bodcprodsvipa111.prod.lattice.local:8073/score/'

#SCORING_ENDPOINT = 'http://10.41.1.180:8080/score'
#OAUTH2_ENDPOINT = 'https://app3.lattice-engines.com/pls/oauth2'
#OAUTH2_ENDPOINT = 'http://10.41.0.52:8080/pls/oauth2'

def scoreLeads(tenant, username, password, leadFileName):

    authorization = getAuthenticationToken(tenant, username, password)
    token = getAccessToken(tenant, authorization)
    modelGUID = getModelGUID(token)
    requiredFieldNames = getRequiredFields(token, modelGUID)
    scoreLeadFile(token, modelGUID, leadFileName, requiredFieldNames)


def getAuthenticationToken(tenant, username, password):
    # print 'Password: ' + password
    tenantID = '{0}.{0}.Production'.format(tenant)
    url = PLS_ENDPOINT + '/login'
    body_login = {"Username": username, "Password": password}
    header_login = {"Content-Type": "application/json"}
    response = requests.post(url, headers=header_login, data=json.dumps(body_login), verify=False)
    assert response.status_code == 200, 'Request failed for logging in to LP.\n' + response.content
    results = json.loads(response.text)
    auth_token = results["Uniqueness"] + '.' + results["Randomness"]

    url_attach = PLS_ENDPOINT + '/attach'
    body_attach = {"Identifier": '%s' % tenantID, "DisplayName": tenant}
    header_attach = {"Content-Type": "application/json", "Authorization": auth_token}
    response_attach = requests.post(url_attach, headers=header_attach, data=json.dumps(body_attach), verify=False)

    assert response_attach.status_code == 200, 'Request failed for logging in to LP.\n' + response_attach.content
    # print 'got the auth token: ' + auth_token
    return auth_token


def getAccessToken(tenant, authorization):
    tenantID = '{0}.{0}.Production'.format(tenant)
    url = PLS_ENDPOINT + '/oauth2/accesstoken?tenantId={0}'.format(tenantID)
    header = {'Authorization': '{0}'.format(authorization)}
    response = requests.get(url, headers=header, verify=False)
    return response.content


def getModelGUID(token):
    url = SCORING_ENDPOINT + '/models/CONTACT'
    header = {'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': 'Bearer {0}'.format(token)}
    response = requests.get(url, headers=header, verify=False)
    models = response.json()

    print ''
    print 'Available Models:'
    print ''
    modelMap = {}
    for m in models:
        type = m['type']
        name = m['name']
        modelId = m['modelId']
        modelMap[name] = modelId
        print '  > {0}'.format(name)

    print ''

    candidates = []
    while True:
        candidates = []
        m = raw_input('Model name for scoring: ')
        if m == '':
            continue
        if m in modelMap:
            candidates.append(m)
            break
        for name in modelMap:
            if name[:len(m)] == m:
                candidates.append(name)
        if len(candidates) < 2:
            break
        print ''
        print 'Possible models are:'
        print ''
        for c in candidates:
            print '  * {0}'.format(c)
        print ''

    return modelMap[candidates[0]]


def getRequiredFields(token, modelGUID):
    url = SCORING_ENDPOINT + '/models/{0}/fields'.format(modelGUID)
    header = {'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': 'Bearer {0}'.format(token)}
    response = requests.get(url, headers=header, verify=False)
    requiredFieldNames = []
    fields = response.json()['fields']
    print ''
    print 'Required Columns:'
    for d in fields:
        fieldName = d['fieldName']
        print ' *', fieldName
        requiredFieldNames.append(fieldName)
    return requiredFieldNames


def scoreLeadFile(token, modelGUID, leadFileName, requiredFieldNames):

    url = SCORING_ENDPOINT + '/record'
    header = {'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': 'Bearer {0}'.format(token)}

    scores = []
    tasks = []
    nLeads = 0

    with codecs.open(leadFileName, encoding='utf-8-sig', mode='rb') as leadFile:

        #fileReader = csv.DictReader(leadFile)
        fileReader = csv.DictReader(utf_8_encoder(leadFile))

        for row in fileReader:
            nLeads += 1
            #if nLeads > 1000:
            #    break
            row['CreatedDate'] = int(time.mktime(datetime.strptime(row['CreatedDate'], '%Y-%m-%d %H:%M:%S').timetuple()))
            requiredFields = {}
            for f in requiredFieldNames:
                requiredFields[f] = row[f]
            record = json.dumps({'record' : requiredFields, 'modelId' : '{0}'.format(modelGUID)})
            postdata = (url, header, record)
            tasks.append(postdata)

    starttime = time.time()
    pool = multiprocessing.Pool()
    r = pool.map_async(scoreLead, tasks, callback=scores.append)
    r.wait()
    endtime = time.time()
    duration = endtime - starttime
    print ''
    print '{0} leads scored in {1} seconds'.format(nLeads, int(duration))

    with open(leadFileName[:-4]+'_SCORES.csv', 'wb') as scoreFile:
        columnNames = ['ID', 'Percentile']
        writer = csv.DictWriter(scoreFile, fieldnames=columnNames)

        writer.writeheader()

        for s in scores[0]:
            (ID, score) = s
            if ID != '0':
                writer.writerow({'ID': ID, 'Percentile': int(score)})


def scoreLead(postdata):
    (url, header, record) = postdata
    (Id, score) = ('0', 0.0)
    try:
        response = requests.post(url, headers=header, data=record, verify=False)
        score = response.json()['score']
        Id = response.json()['id']
    except:
        print 'Scoring Error for record:\n{0}'.format(record)
    return (Id, score)


def utf_8_encoder(unicode_csv_data):
    for line in unicode_csv_data:
        yield line.encode('utf-8')


def usage(cmd, exit_code):
    path = ''
    i = cmd.rfind('\\')
    j = cmd.rfind('/')
    if( i != -1 ):
        path = cmd[:i]
        cmd = cmd[i+1:]
    elif( j != -1 ):
        path = cmd[:j]
        cmd = cmd[j+1:]

    print ''
    print 'PATH: {0}'.format(path)
    print 'Usage: {0} <tenant> <username> <password> <leadfile.csv>'.format(cmd)
    print ''

    exit(exit_code)


if __name__ == "__main__":

    if len(sys.argv) == 1:
        usage(sys.argv[0], 0)

    if len(sys.argv) != 5:
        usage(sys.argv[0], 1)

    tenant = sys.argv[1]
    username = sys.argv[2]
    password = hashlib.sha256(sys.argv[3]).hexdigest()
    leadFileName = sys.argv[4]

    scoreLeads(tenant, username, password, leadFileName)
