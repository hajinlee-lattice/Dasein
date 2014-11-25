'''
Created on Oct 17, 2014

@author: hliu
'''

from modeling.modelingjob import ModelingJob
from rest.restclient import sendGetRequest
import time

class GetJobStatus(ModelingJob):
    
    def submitJob(self, config, appId, restEndpointHost):
        return sendGetRequest("getJobStatus/" + appId, restEndpointHost)
    
    def pollJobStatus(self, appId, restEndpointHost):
        jobReport = self.submitJob(None, appId, restEndpointHost)
        status = jobReport["status"]
        diagnostics = jobReport["diagnostics"]
        while status is not None:
            if self.isSucceeded(status):
                print "ModelingJob " + appId + " Succeeded!"
                return jobReport["status"]
            elif self.isKilled(status):
                print "ModelingJob " + appId + " Killed!"
                return jobReport["status"]
            elif self.isFailed(status, diagnostics):
                print "ModelingJob " + appId + " Failed!"
                return jobReport["status"]
            elif self.isPreempted(diagnostics):
                print "ModelingJob " + appId + " Preempted! Please wait for the job resubmission to relaunch the job."
            print "ModelingJob " + appId + " state: " + jobReport["state"] + ", status: " + status + ", progress: " + str(jobReport["progress"])
            time.sleep(5)
            jobReport = self.submitJob(None, appId, restEndpointHost)
            status = jobReport["status"]
            diagnostics = jobReport["diagnostics"]
    
    
    def isSucceeded(self, status):
        return status == "SUCCEEDED"
    
    def isKilled(self, status):
        return status == "KILLED"
    
    def isFailed(self, status, diagnostics):
        if status == "FAILED" and not self.isPreempted(diagnostics): return True
        return False
            
    def isPreempted(self, diagnostics):
        if diagnostics is None or len(diagnostics) == 0: return False
        return diagnostics.find("-102") > 0 and diagnostics.find("Container preempted by scheduler") > 0;