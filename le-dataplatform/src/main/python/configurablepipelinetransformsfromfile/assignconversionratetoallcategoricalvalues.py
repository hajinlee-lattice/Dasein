'''
Description:
If the choice is made to assign a categorical variable to numeric values, the choice should be the conversion rate for that specitic categorical value.
For example, category A has 0% conversion, B has 1% conversion, and C has 8% conversion in the training set.  Reassign A,B, and C to 0,.01 and .08, respectively
'''
from collections import Counter
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
        totalPositiveEventCount = float(sum(dataFrame[self.targetColumn]))
        meanConversionRate = round(totalPositiveEventCount / float(len(dataFrame[self.targetColumn])), 2)
        if meanConversionRate == 0.0:
            for i in xrange(3, 5):
                meanConversionRate = round(totalPositiveEventCount / float(len(dataFrame[self.targetColumn])), i)
                if meanConversionRate != 0.0:
                    break
        
        for column, _ in self.categoricalColumns.iteritems():
            if column in dataFrame.columns:
                self.currentColumn = column
                
                if not test:
                    self.__assignConversionRateToCategoricalColumns(column, dataFrame, meanConversionRate)
                    self.__writeRTSArtifacts()
                dataFrame[column] = dataFrame[column].apply(self.__applyConversionRate)

        return dataFrame

    def __assignConversionRateToCategoricalColumns(self, column, dataFrame, meanConversionRate):
        if self.targetColumn in dataFrame and len(dataFrame[column]) == len(dataFrame[self.targetColumn]):
            logger.info("AssignConversionRate training phase. Converting column: " + column)
            workingColumn = list(dataFrame[column])
            cCount = Counter(workingColumn)
            targetColumn = list(dataFrame[self.targetColumn])
            posCount = Counter([workingColumn[i] for i in range(len(workingColumn)) if targetColumn[i] == 1])
            yy = { k:(cCount[k], float(posCount[k])) for k in posCount.keys() }
            yy2 = { k:(cCount[k], 0.0) for k in set(cCount.keys()) - set(posCount.keys()) }
            yy = dict(yy.items() + yy2.items())
            def conversionRateCalc(k):
                if yy[k][0] == 0 or yy[k][1] < self.totalPositiveThreshold:
                    return 1.0
                return (yy[k][1] / yy[k][0]) / meanConversionRate
            keyConversionRate = { k:conversionRateCalc(k) for k in yy.keys() }
            self.categoricalColumnMapping[column] = keyConversionRate

    def __applyConversionRate(self, categoryValue):
        if self.currentColumn in self.categoricalColumnMapping:
            if pd.isnull(categoryValue):
                return 1.0
            if categoryValue in self.categoricalColumnMapping[self.currentColumn]:
                return self.categoricalColumnMapping[self.currentColumn][categoryValue]
        return 1.0

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
        return [{"applyconversionratestep-conversionratemapping.json": self.categoricalColumnMapping}]

    def getOutputColumns(self):
        return [(create_column(k, "FLOAT"), [k]) for k, _ in self.categoricalColumnMapping.iteritems()]

    def getRTSMainModule(self):
        return "assignconversionrate"
