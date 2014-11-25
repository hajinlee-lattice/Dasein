'''
Created on Oct 23, 2014

@author: hliu
'''
from unittest import TestCase

from domain.classdictgenerator import initDict
from modeling.createsamples import CreateSamples
from modeling.getjobstatus import GetJobStatus
from modeling.loaddata import LoadData
from modeling.profiledata import ProfileData
from modeling.setalgorithm import SetAlgorithm
from modeling.submitmodel import SubmitModel


class WorkFlowTest(TestCase):

    def setUp(self):
        classDict = initDict("../../../main/resources/jsonschema/")
        self.restEndpointHost = "localhost:8080"
        webHdfs = "http://localhost:50070"
        self.loadData = LoadData(classDict)
        self.loadData.setWebHDFS(webHdfs)
        self.createSamples = CreateSamples(classDict)
        self.createSamples.setWebHDFS(webHdfs)
        self.profileData = ProfileData(classDict)
        self.profileData.setWebHDFS(webHdfs)
        self.submitModel = SubmitModel(classDict)
        self.submitModel.setWebHDFS(webHdfs)
        self.jobStatus = GetJobStatus(classDict)
        self.setAlgorithm = SetAlgorithm(classDict)
        
        customer = "DataScientistTest"
        table = "Q_EventTable_Nutanix"
        targets = ["P1_Event"]
        metadata_table = "EventMetadata"
        key_columns = ["Nutanix_EventTable_Clean"]
        
        self.loadData.rmDir("/user/s-analytics/customers/DataScientistTest")
        creds = {"host":"localhost", "port":"3306", "db":"LeadScoringDB", "user":"root", "password":"welcome", "db_type":"MySQL"}
        self.loadConfiguration = {"creds":creds, "customer":customer, "table":table, "key_columns":key_columns}
        
        self.samplingConfiguration = {"customer": customer, "table": table, "training_percentage": 80}
        self.samplingConfiguration["sampling_elements"] = self.createSamples.createSampleElements(3)
        
        self.dataProfileConfiguration = {"customer": customer, "table": table, "targets": targets, "sample_prefix":"all", "metadata_table": metadata_table, "exclude_list": ["P1_Event"]}
        
        rf1_algorithm = {"name": "rf_1", "priority":0, "sample":"all", "type": "randomForestAlgorithm", "container_properties": "VIRTUALCORES=1 MEMORY=64 PRIORITY=0" }
        rf2_algorithm = {"name": "rf_2", "priority":1, "sample":"all", "type": "randomForestAlgorithm", "container_properties": "VIRTUALCORES=1 MEMORY=64 PRIORITY=0"}
        model_definition = {"name": "Model Definition", "algorithms":[rf1_algorithm, rf2_algorithm]}
        self.model = {"data_format":"avro","name": "testmodel", "customer": customer, "table": table, "targets": targets, "key_columns":key_columns, "metadata_table": metadata_table, "model_definition": model_definition}
        
    def testWorkFlow(self):
        appId = self.loadData.submitJob(self.loadConfiguration, self.restEndpointHost)["application_ids"][0]
        self.assertEqual(appId.find("application"), 0)
        self.assertEqual(self.jobStatus.pollJobStatus(appId, self.restEndpointHost), "SUCCEEDED")
        self.assertEqual(self.loadData.submitJob(self.loadConfiguration, self.restEndpointHost), "Passed")
        
        appId = self.createSamples.submitJob(self.samplingConfiguration, self.restEndpointHost)["application_ids"][0]
        self.assertEqual(appId.find("application"), 0)
        self.assertEqual(self.jobStatus.pollJobStatus(appId, self.restEndpointHost), "SUCCEEDED")
        self.assertEqual(self.createSamples.submitJob(self.samplingConfiguration, self.restEndpointHost), "Passed")
        
        appId = self.profileData.submitJob(self.dataProfileConfiguration, self.restEndpointHost)["application_ids"][0]
        self.assertEqual(appId.find("application"), 0)
        self.assertEqual(self.jobStatus.pollJobStatus(appId, self.restEndpointHost), "SUCCEEDED")
        
        appIds = self.submitModel.submitJob(self.model, self.restEndpointHost)["application_ids"]
        for appId in appIds:
            self.assertEqual(appId.find("application"), 0)
            self.assertEqual(self.jobStatus.pollJobStatus(appId, self.restEndpointHost), "SUCCEEDED")
