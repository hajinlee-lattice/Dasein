'''
Description:
If the choice is made to assign a categorical variable to numeric values, the choice should be the conversion rate for that specitic categorical value.
For example, category A has 0% conversion, B has 1% conversion, and C has 8% conversion in the training set.  Reassign A,B, and C to 0,.01 and .08, respectively
'''
from json import encoder
import json
import os
import pandas as pd
from pipelinefwk import PipelineStep
from pipelinefwk import create_column
from pipelinefwk import get_logger


logger = get_logger("pipeline")

class AssignConversionRateToAllCategoricalValues(PipelineStep):

    columnList = []
    categoricalColumnMapping = None
    categoricalColumnMappingFilePath = None
    totalPositiveThreshold = 12
    categoricalColumns = {}
    targetColumn = ""

    def __init__(self, categoricalColumns, targetColumn, totalPositiveThreshold=12):
        self.categoricalColumns = categoricalColumns
        self.targetColumn = targetColumn
        self.totalPositiveThreshold = totalPositiveThreshold
        
        if self.categoricalColumnMapping is None:
            self.categoricalColumnMapping = {}
        if categoricalColumns:
            self.columnList = categoricalColumns.keys()
        else:
            self.columnList = []
        logger.info("Initialized AssignConversionRate with categoricalColumns " + str(categoricalColumns)
                    + ", targetColumn=" + str(targetColumn)
                    + ", positiveThreshold=" + str(self.totalPositiveThreshold))
    
    def transform(self, dataFrame, configMetadata, test):
        for column, _ in self.categoricalColumns.iteritems():
            if column in dataFrame.columns:
                self.currentColumn = column
                self.__assignConversionRateToCategoricalColumns(column, dataFrame, self.targetColumn)
                dataFrame[column] = dataFrame[column].apply(self.__applyConversionRate)
        self.__writeRTSArtifacts()
        return dataFrame

    def __assignConversionRateToCategoricalColumns(self, column, dataFrame, targetColumn):
        if self.targetColumn in dataFrame and len(dataFrame[column]) == len(dataFrame[self.targetColumn]):
            logger.info("AssignConversionRate training phase. Converting column: " + column)
            allKeys = set(dataFrame[column])
            listOfValues = [x for x in enumerate(dataFrame[column])]
            totalPositiveEventCount = float(sum(dataFrame[self.targetColumn]))
            meanConversionRate = round(totalPositiveEventCount / (len(dataFrame[self.targetColumn])), 2)
            keyConversionRate = {}
            for key in allKeys:
                ind = [i for i, x in listOfValues if x == key]
                count = len(ind)
                if count > 0:
                    totalPositives = sum([dataFrame[self.targetColumn].iloc[i] for i in ind])
                    if totalPositives >= self.totalPositiveThreshold:
                        perc = round(sum([dataFrame[self.targetColumn].iloc[i] for i in ind]) * 1.0 / count, 2)
                        keyConversionRate[key] = perc
                    else:
                        keyConversionRate[key] = meanConversionRate
                else:
                    keyConversionRate[key] = 0.0
            keyConversionRate["UNKNOWN"] = meanConversionRate
            self.categoricalColumnMapping[column] = keyConversionRate

    def __applyConversionRate(self, categoryValue):
        if self.currentColumn in self.categoricalColumnMapping:
            if pd.isnull(categoryValue):
                return self.categoricalColumnMapping[self.currentColumn]["UNKNOWN"]
            if categoryValue in self.categoricalColumnMapping[self.currentColumn]:
                if self.categoricalColumnMapping[self.currentColumn][categoryValue]:
                    return self.categoricalColumnMapping[self.currentColumn][categoryValue]
                else:
                    return self.categoricalColumnMapping[self.currentColumn]["UNKNOWN"]
        return categoryValue

    def __writeRTSArtifacts(self):
        e = encoder.FLOAT_REPR
        encoder.FLOAT_REPR = lambda o: format(o, ".2f")
        with open("conversionratemapping.json", "wb") as fp:
            json.dump(self.categoricalColumnMapping, fp)
            self.categoricalColumnMappingFilePath = os.path.abspath(fp.name)
        encoder.FLOAT_REPR = e

    def getRTSArtifacts(self):
        return [("conversionratemapping.json", self.categoricalColumnMappingFilePath)]

    def doColumnCheck(self):
        return False

    def getDebugArtifacts(self):
        return [{"categoricalmapping.json": self.categoricalColumnMapping}]

    def getOutputColumns(self):
        return [(create_column(k, "FLOAT"), [k]) for k, _ in self.categoricalColumnMapping.iteritems()]

    def getRTSMainModule(self):
        return "assignconversionrate"
