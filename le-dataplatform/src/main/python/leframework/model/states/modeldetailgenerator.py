from collections import OrderedDict
import calendar
import logging
import time

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

        result["Name"] = mediator.schema["name"]
        result["LookupID"] = self.lookupID()

        # Leads
        result["TotalLeads"] = mediator.allDataPreTransform.shape[0]
        result["TestingLeads"] = mediator.data.shape[0]
        result["TrainingLeads"] = result["TotalLeads"] - result["TestingLeads"]

        # Conversions
        result["TotalConversions"] = int(mediator.allDataPreTransform[schema["target"]].sum())
        result["TestingConversions"] = int(mediator.data[schema["target"]].sum())
        result["TrainingConversions"] = result["TotalConversions"] - result["TestingConversions"]

        if self.mediator.rocscore is not None:
            result["RocScore"] = self.mediator.rocscore

        result["ConstructionTime"] = self.now()

        # Add Result to Mediator
        self.mediator.modeldetails = result

    def now(self):
        return calendar.timegm(time.gmtime())

    # Current Path Structure: /user/s-analytics/customers/<customer>/models/<eventTableName>/<guid>/
    def lookupID(self):
        result = None
        customerOffset = 4; eventTableOffset = 6; guidOffset = 7
        if self.mediator.modelHdfsDir != None:
            tokens = self.mediator.modelHdfsDir.split("/")
            if len(tokens) > 7:
                result = tokens[customerOffset] + "|" + tokens[eventTableOffset] + "|" + tokens[guidOffset]
        return result
