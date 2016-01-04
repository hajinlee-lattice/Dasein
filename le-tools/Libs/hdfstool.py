'''
Created on Dec 17, 2015

@author: smeng
'''
import requests
import json
import logging


baseUrl = 'http://10.41.1.106:14000/webhdfs/v1'
hdfsUser = '&user.name=hdfs'


def listPath(path):
    op = 'LISTSTATUS'
    url = _getUrl(path, op)
    r = requests.get(url)

    if 200 != r.status_code:
        logging.error('Request failed for ' + op + '\n' + r.content)
        return

    directoriesJson = json.loads(r.text)['FileStatuses']['FileStatus']
    directories = []
    for dirJson in directoriesJson:
        directories.append(dirJson['pathSuffix'])
    return json.dumps(directories)

def exists(path):
    op = 'GETFILESTATUS'
    url = _getUrl(path, op)
    r = requests.get(url)
    if 200 != r.status_code and 'java.io.FileNotFoundException' in r.content:
        return False
    elif json.loads(r.text)['FileStatus'] != None:
        return True
    else:
        logging.error('Request failed for ' + op + '\n' + r.content)
        return False

def _getUrl(path, op):
    return baseUrl + path + '?op=' + op + hdfsUser

if __name__ == '__main__':
    # print listPath('/user/s-analytics/customers/20151125001409488/models/Play_30_Analysis_635840136317430902_Construction/81480e7d-0d5c-44fd-9e39-a3617e16b507/metadata2.json')
    # print exists('/user/s-analytics/customers/20151125001409488/models/Play_30_Analysis_635840136317430902_Construction/81480e7d-0d5c-44fd-9e39-a3617e16b507')
    pass
