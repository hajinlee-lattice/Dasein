'''
Created on Oct 21, 2014

@author: hliu
'''
import pwd, os
from urlparse import urlparse

from webhdfs import WebHDFS


class ModelingJob(object):

    def __init__(self, classDict={}):
        self.longOpts = []
        self.hdfs = ""
        
        
    def getLongOptions(self): return self.longOpts
        
    def generateConfiguration(self, opts, config=None): pass
    
    def populateValueFromOption(self, opt, arg, objDict):
        for key in objDict.viewkeys():
            if key == str(opt).strip("--"):
                if (str(objDict[key].__repr__).find("list") > 0): objDict[key] = arg.split(",")
                else: objDict[key] = arg
        
    def submitJob(self, configuration, restEndpointHost): pass
    
    def setWebHDFS(self, url):
        webHdfsHostPort = urlparse(url)
        self.hdfs = WebHDFS(webHdfsHostPort.hostname, webHdfsHostPort.port, pwd.getpwuid(os.getuid())[0])
        
    def listFiles(self, path): return self.hdfs.listdir(path)
    
    def rmDir(self, path): return self.hdfs.rmdir(path)
    
    def addFile(self, source_path, target_path): self.hdfs.copyFromLocal(source_path, target_path)
    
    def getFile(self, source_path, target_path): self.hdfs.copyToLocal(source_path, target_path)