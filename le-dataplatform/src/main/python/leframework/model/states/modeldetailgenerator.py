import calendar
import logging
import time
import json
from collections import OrderedDict
from leframework.codestyle import overrides
from leframework.model.state import State

class ModelDetailGenerator(State):

    def __init__(self):
        State.__init__(self, "ModelDetailGenerator")
        self.logger = logging.getLogger(name='modeldetailgenerator')

    @overrides(State)
    def execute(self):
        mediator = self.mediator
        schema = mediator.schema
        result = OrderedDict()
        mediator.modelId = self.generateModelID()

        result["Name"] = mediator.schema["name"]
        if mediator.schema.has_key("display_name"):
            result["DisplayName"] = mediator.schema["display_name"]
        result["LookupID"] = self.lookupID()
        result["ModelID"] = mediator.modelId
        # Leads
        allData = mediator.allDataPreTransform
        testData = mediator.data
        result["TestingLeads"] = testData.shape[0]
        # #PLS-4158 cannot simply update total counts here
        # if '__TRAINING__' in allData.columns.values:
        #     trainingData = allData[allData['__TRAINING__'] == 1]
        #     allData = DataFrame.append(trainingData, testData)
        #     result["TrainingLeads"] = trainingData.shape[0]
        #     result["TotalLeads"] = result["TrainingLeads"] + result["TestingLeads"]
        # else:
        result["TotalLeads"] = allData.shape[0]
        result["TrainingLeads"] = result["TotalLeads"] - result["TestingLeads"]

        # Conversions
        result["TotalConversions"] = int(allData[schema["target"]].sum())

        if "__Revenue" in allData.columns:
            try:
                if schema.has_key("config_metadata") and schema["config_metadata"].has_key("Metadata"):
                    for me in schema["config_metadata"]["Metadata"]:
                        if me["ColumnName"] == "__Revenue":
                            dataType = me["DataType"]
                            if dataType in ["long", "int", "double", "float"]:
                                self.logger.info("Calculating 'AverageRevenue' using '__Revenue'")
                                result["AverageRevenue"] = float(allData["__Revenue"].sum()) / allData.shape[0]
                            else:
                                self.logger.info("Skip calculation of 'AverageRevenue' using '__Revenue'")
                            break
            except AttributeError:
                self.logger.info("Got AttributeError as it may be for CG modelingi (PLS-11853), attempting to calculate AverageRevenue normally", exc_info=True)
                try:
                    result["AverageRevenue"] = float(allData["__Revenue"].sum()) / allData.shape[0]
                except:
                    self.logger.info("Ignoring secondary error as it may be for CG modeling (PLS-11853), skip calculation of AverageRevenue", exc_info=True)

        result["TestingConversions"] = int(testData[schema["target"]].sum())
        result["TrainingConversions"] = result["TotalConversions"] - result["TestingConversions"]
        result["ModelType"] = mediator.modelType.split(":")[0]
        try:
            if self.mediator.rocscore is not None:
                result["RocScore"] = self.mediator.rocscore
        except AttributeError:
            result["RocScore"] = -1

        result["ConstructionTime"] = self.now()
        result["TemplateVersion"] = mediator.templateVersion
        
        # Add Result to Mediator
        self.mediator.modeldetails = result

    def now(self):
        return calendar.timegm(time.gmtime())

    def generateModelID(self):
        schema = self.mediator.schema
        idx = schema["model_data_dir"].rfind('/') + 1
        modelId = "ms__" + schema["model_data_dir"][idx:] + "-" + schema["name"]
        return modelId if len(modelId) <= 49 else modelId[:49]

    # Current Path Structure: /user/s-analytics/customers/<customer>/models/<eventTableName>/<guid>/
    def lookupID(self):
        result = None
        customerOffset = 4; eventTableOffset = 6; guidOffset = 7
        if self.mediator.modelHdfsDir != None:
            tokens = self.mediator.modelHdfsDir.split("/")
            if len(tokens) > 7:
                result = tokens[customerOffset] + "|" + tokens[eventTableOffset] + "|" + tokens[guidOffset]
        return result
