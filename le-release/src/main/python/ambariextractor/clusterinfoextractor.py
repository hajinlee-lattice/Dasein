import base64
import json
import os
import requests
import shutil


class ClusterInfoExtractor(object):
    '''
    classdocs
    '''
    
    def __init__(self): pass

    def extract(self, ambariserver, ambariport, username, password, clustername):
        url = "http://%s:%s/api/v1/clusters/%s" % (ambariserver, ambariport, clustername)
        creds = "%s:%s" % (username, password)
        authorization = "Basic %s" % base64.b64encode(bytearray(creds)) 
        headers = { "Authorization": authorization, "X-Requested-By": "Ambari"}
        r = requests.get(url, headers=headers)
        clusterjson = json.loads(r.content)
        
        shutil.rmtree("./hosts", ignore_errors=True)
        os.mkdir("./hosts")
        for host in clusterjson["hosts"]:
            r = requests.get(host["href"], headers=headers)
            with open("./hosts/%s.json" % (host["Hosts"]["host_name"]), "w") as hostwrite:
                hostwrite.write(r.content)

        shutil.rmtree("./configurations", ignore_errors=True)
        os.mkdir("./configurations")
        for config in clusterjson["configurations"]:
            r = requests.get(config["href"], headers=headers)
            with open("./configurations/%s-%s.json" % (config["type"], config["tag"]), "w") as configwrite:
                configwrite.write(r.content)
            
        
        


