'''
Created on Oct 20, 2014

@author: hliu
'''

import sys, getopt
from domain.classdictgenerator import initDict
from modeling.loaddata import LoadData
from modeling.createsamples import CreateSamples
from modeling.profiledata import ProfileData
from modeling.setalgorithm import SetAlgorithm
from modeling.submitmodel import SubmitModel
from modeling.getjobstatus import GetJobStatus
from modeling.modelingjob import ModelingJob

def main():
    classDict = initDict("../resources/jsonschema/") 
    algorithms = []
    webHdfs = ""
    while True:
        try:
            line = sys.stdin.readline().strip("\n")
            tokens = line.split(" ")
            commandType = tokens[0]
            restEndpointHost = tokens[1]
            config = None
            '''
                Need to SetWebHDFS first: "http://localhost:50070"
            '''
            modelingJob = ModelingJob(classDict)
            modelingJob.setWebHDFS(webHdfs) 
            if commandType == "SetWebHDFS": webHdfs = tokens[1]
            elif commandType == "Delete": modelingJob.rmDir(tokens[1])
            elif commandType == "Upload": modelingJob.addFile(tokens[1], tokens[2])
            elif commandType == "Download": modelingJob.getFile(tokens[1], tokens[2])
            
            else:
                if commandType == "SetAlgorithm": args = tokens[1:]
                else: args = tokens[2:]
                
                obj = globals()[commandType](classDict)
                obj.setWebHDFS(webHdfs)
                longOpts = obj.getLongOptions()
                opts, args = getopt.getopt(args, "", longOpts)
                config = obj.generateConfiguration(opts, algorithms)
                if commandType == "SetAlgorithm": algorithms.append(config)
                else:
                    appIds = obj.submitJob(config, restEndpointHost)
                    if appIds != "Passed":
                        getJobStatus = GetJobStatus(classDict)
                        for appId in appIds["application_ids"]: getJobStatus.pollJobStatus(appId, restEndpointHost)
                    
        except KeyboardInterrupt:
            break
        if not line:
            break


if __name__ == '__main__':
    main()
