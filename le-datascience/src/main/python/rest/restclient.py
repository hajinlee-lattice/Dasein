'''
Created on Oct 14, 2014

@author: hliu
'''
import json,urllib2 

def sendPutRequest(configuration, jobType, restEndpointHost):
    url = "http://" + restEndpointHost + "/rest/" + jobType
    data = json.dumps(configuration, ensure_ascii=False)
    req = urllib2.Request(url, data, {'Content-Type': 'application/json'})
    f = urllib2.urlopen(req)
    response = f.read()
    f.close()
    return json.loads(response)

def sendGetRequest(jobType, restEndpointHost):
    url = "http://" + restEndpointHost + "/rest/" + jobType
    f = urllib2.urlopen(url)
    response = f.read()
    f.close()
    return json.loads(response)
    
