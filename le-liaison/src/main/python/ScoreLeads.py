#!/usr/bin/python

import sys, requests, codecs, csv, json
from datetime import datetime
import time
import multiprocessing
import subprocess

#SCORING_ENDPOINT = 'http://api2.lattice.local/score'
SCORING_ENDPOINT = 'http://10.41.1.180:8080/score'
#OAUTH2_ENDPOINT = 'http://10.41.0.16:8072/'
OAUTH2_ENDPOINT = 'http://10.41.0.52:8080/pls/oauth2'

def scoreLeads(tenantID, authorization, leadFileName):

    token = getAccessToken(tenantID, authorization)
    modelGUID = getModelGUID(token)
    requiredFieldNames = getRequiredFields(token, modelGUID)
    scoreLeadFile(token, modelGUID, leadFileName, requiredFieldNames)


def getAccessToken(tenantID, authorization):
    url = OAUTH2_ENDPOINT + '/accesstoken?tenantId={0}'.format(tenantID)
    header = {'Authorization': '{0}'.format(authorization)}
    response = requests.get(url, headers=header)
    return response.content


def getModelGUID(token):
    url = SCORING_ENDPOINT + '/models/CONTACT'
    header = {'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': 'Bearer {0}'.format(token)}
    response = requests.get(url, headers=header)
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
    response = requests.get(url, headers=header)
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

    with open('scores.csv', 'w') as scoreFile:
        columnNames = ['ID', 'Percentile']
        writer = csv.DictWriter(scoreFile, fieldnames=columnNames)

        writer.writeheader()

        for s in scores[0]:
            (ID, score) = s
            writer.writerow({'ID': ID, 'Percentile': int(score)})


def scoreLead(postdata):
    (url, header, record) = postdata
    response = requests.post(url, headers=header, data=record)
    score = response.json()['score']
    Id = response.json()['id']
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
    print 'Usage: {0} <TenantID> <Authorization> <LeadFile.csv>'.format(cmd)
    print ''

    exit(exit_code)


if __name__ == "__main__":

    if len(sys.argv) == 1:
        usage(sys.argv[0], 0)

    if len(sys.argv) != 4:
        usage(sys.argv[0], 1)

    tenantID = sys.argv[1]
    authorization = sys.argv[2]
    leadFileName = sys.argv[3]

    scoreLeads(tenantID, authorization, leadFileName)
