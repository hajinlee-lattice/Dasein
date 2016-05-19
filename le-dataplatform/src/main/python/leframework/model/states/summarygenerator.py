from collections import OrderedDict
from datetime import datetime
import logging
import time

from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State


class SummaryGenerator(State, JsonGenBase):

    def __init__(self):
        State.__init__(self, "SummaryGenerator")
        self.logger = logging.getLogger(name = 'summarygenerator')

    @overrides(State)
    def execute(self):
        mediator = self.mediator
        self.summary = OrderedDict()
        self.summary["SchemaVersion"] = 1
        self.summary["Predictors"] = mediator.predictors

        rocScore = mediator.rocscore

        if rocScore is not None:
            self.summary["RocScore"] = rocScore
        self.summary["SegmentChart"] = self.__getSegmentChart(mediator.probRange, mediator.widthRange, mediator.buckets, mediator.averageProbability)
        self.summary["DLEventTableData"] = self.__getDLEventTableData(self.mediator.provenanceProperties, mediator.allDataPreTransform.shape[0])
        self.summary["ConstructionInfo"] = self.__getConstructionInfo()
        self.summary["ModelID"] = mediator.modelId

    @overrides(JsonGenBase)
    def getKey(self):
        return "Summary"
    
    @overrides(JsonGenBase)
    def getJsonProperty(self):
        return self.summary

    def __getSegmentChart(self, probRange, widthRange, buckets, averageProbability):
        # Generate inclusive (min,max) with highest max = null and lowest min = 0
        if len(probRange) == 1:
            inclusive = [(0, None)]
        else:
            inclusive = [((probRange[0] + probRange[1]) / 2, None)]
            for i in range (1, len(probRange) - 1):
                inclusive.append(((probRange[i] + probRange[i + 1]) / 2, inclusive[i - 1][0]))
            inclusive.append((0, inclusive[len(probRange) - 2][0]))
    
        # Generate name for each segment
        names = []
        for i in xrange(len(probRange)):
            curProb = probRange[i] if self.mediator.type == 0 else probRange[i] / averageProbability
            for j in xrange(len(buckets)): 
                if buckets[j]["Minimum"] is not None and buckets[j]["Maximum"] is not None:
                    if curProb >= buckets[j]["Minimum"] and curProb < buckets[j]["Maximum"]:
                        names.append(buckets[j]["Name"])
                        break
                elif buckets[j]["Minimum"] is not None and curProb >= buckets[j]["Minimum"]:
                        names.append(buckets[j]["Name"])
                        break
                elif buckets[j]["Maximum"] is not None and curProb < buckets[j]["Maximum"]:    
                        names.append(buckets[j]["Name"])
                        break
           
        # Generate segments
        segments = []
        for i in xrange(len(probRange)):
            element = OrderedDict()
            element["AverageProbability"] = probRange[i]
            element["LowerInclusive"] = inclusive[i][0]
            element["Name"] = names[i]
            element["UpperExclusive"] = inclusive[i][1]
            element["Width"] = widthRange[i]
            segments.append(element)
     
        # Generate segment chart
        segmentChart = OrderedDict()
        segmentChart["AverageProbability"] = averageProbability
        segmentChart["Segments"] = segments

        return segmentChart

    def __getDLEventTableData(self, provenanceProperties, rowCount):
        element = OrderedDict()
        element["SourceRowCount"] = rowCount
        if len(provenanceProperties) < 3:
            self.logger.error("Provenance property does not have the dataloader properties.")
            return element

        if "DataLoader_Instance" in provenanceProperties:
            element["DataLoaderURL"] = provenanceProperties["DataLoader_Instance"]
        if "DataLoader_TenantName" in provenanceProperties:
            element["TenantName"] = provenanceProperties["DataLoader_TenantName"]
        if "DataLoader_Query" in provenanceProperties:
            element["QueryName"] = provenanceProperties["DataLoader_Query"]
        return element

    def __getConstructionInfo(self):
        constructionTime = OrderedDict()
        # DateTime returns UTC epoch in milliseconds
        constructionTime["DateTime"] = "/Date(" + str(int(time.time() * 1000)) + ")/"
        # OffsetMinutes returns UTC offset in current time zone in minutes
        constructionTime["OffsetMinutes"] = str(int((datetime.today() - datetime.utcnow()).total_seconds()) / 60)
        element = OrderedDict()
        element["Source"] = 1
        element["ConstructionTime"] = constructionTime
        element["VersionNumber"] = 1

        return element
