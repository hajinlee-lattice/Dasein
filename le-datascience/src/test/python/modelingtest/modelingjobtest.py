'''
Created on Oct 23, 2014

@author: hliu
'''
import unittest,os
from modeling.modelingjob import ModelingJob

class ModelingJobTest(unittest.TestCase):        
        
    def testWebHdfs(self):
        self.modelingJob = ModelingJob({})
        self.modelingJob.setWebHDFS("http://localhost:50070")
        self.assertEqual(len(self.modelingJob.listFiles("/tmp/modeltest.json")), 0)
        self.modelingJob.addFile("../../resources/jsonschema/model.json", "/tmp/modeltest.json")
        self.modelingJob.getFile("/tmp/modeltest.json", "modeltest.json")
        self.modelingJob.rmDir("/tmp/modeltest.json")
        os.remove("modeltest.json")



    


    