'''
Description:

    This step will pivot the list of columns by retrieving the metadata from our config metadata structure
'''
import os

from pipelinefwk import PipelineStep
from pipelinefwk import create_column
from pipelinefwk import get_logger

from scipy.stats import chisquare


logger = get_logger("pipeline")

class PivotStep(PipelineStep):
    
    columnsToPivot = {}
    categoricalColumns = {}
    dataprofile = {}
    pivotValuesFilePath = None
    minCategoricalCount = 5
    maxCategoricalCount = 10
    
    def __init__(self, columnsToPivot={}, 
                        categoricalColumns={}, 
                        dataprofile={}, 
                        pvalueThreshold=0.10,
                        minCategoricalCount=5,
                        maxCategoricalCount=10):
        self.columnsToPivot = columnsToPivot
        self.categoricalColumns = categoricalColumns
        self.dataprofile = dataprofile
        self.pvalueThreshold = pvalueThreshold
        self.minCategoricalCount = minCategoricalCount
        self.maxCategoricalCount = maxCategoricalCount
        
    def learnParameters(self, trainingDataFrame, testDataFrame, configMetadata):
        learnedConfig = self.__learnPivotValuesFromData(trainingDataFrame, configMetadata)
        self.__setPivotColumns(configMetadata, learnedConfig)
        self.__writeRTSArtifact()

    def transform(self, dataFrame, configMetadata, test):
        columnsToRemove = set()
        for k, v in self.columnsToPivot.items():
            values = v[1]
            columnsToRemove.add(v[0])
            dataFrame[k] = dataFrame[v[0]].apply(lambda row: self.pivot(row, values))
            self.__appendMetadataEntry(configMetadata, k)
        
        super(PivotStep, self).removeColumns(dataFrame, columnsToRemove)
        return dataFrame
    
    def __learnPivotValuesFromData(self, trainingDataFrame, configMetadata):
        if configMetadata is None:
            return
        learnedConfig = {}
        pivotedConfig = self.__getPivotedAttrByConfig(configMetadata)
        for column in self.categoricalColumns:
            if column in pivotedConfig or column not in trainingDataFrame.columns:
                continue
            observed = []
            observedValues = []
            expected = []
            
            categoricalValues = self.dataprofile[column]
            
            if len(categoricalValues) < self.minCategoricalCount or len(categoricalValues) > self.maxCategoricalCount:
                continue 
            for value in categoricalValues:
                if "positiveEventCount" in value:
                    observed.append(value["positiveEventCount"])
                else:
                    continue
                observedValues.append(value["columnvalue"])
                expected.append(float(value["count"]))
            totalObserved = sum(observed)
            totalExpected = sum(expected)
            o = [100.0*x/totalObserved for x in observed]
            e = [100.0*x/totalExpected for x in expected]
            c = chisquare(o, e)[1]
                
            if c < self.pvalueThreshold:
                logger.info("Pivoting %s because chi-square returns %f < %f." % (column, c, self.pvalueThreshold))
                learnedConfig[column] = self.__getPivotConfig(column, observedValues)
                
        return learnedConfig
                
    def __getPivotConfig(self, column, values):
        pivotValues = [{"PivotValue": v, \
                        "PivotColumn": "%s_%s" % (column, ''.join(e for e in v if e.isalnum() or e == '_')) if v is not None else "", \
                        "IsNull": v is None} for v in values]
        return {"Extensions": [{"Key": "PivotValues", "Value": pivotValues}]}
    
    def __getPivotedAttrByConfig(self, configMetadata):
        pivotedConfig = set()
        for config in configMetadata:
            column = config["ColumnName"]
            if "Extensions" in config and config["Extensions"] is not None:
                for e in config["Extensions"]:
                    if e["Key"] == "PivotValues":
                        pivotedConfig.add(column)
        return pivotedConfig

    def __appendMetadataEntry(self, configMetadata, columnName):
        if configMetadata is None:
            return
        entry = {}
        entry["ColumnName"] = columnName
        entry["StatisticalType"] = "ratio"
        entry["FundamentalType"] = "numeric"
        entry["DataType"] = "Integer"
        super(PivotStep, self).appendMetadataEntry(configMetadata, entry)
    
    def pivot(self, row, values):
        for value in values:
            if row == value:
                return 1.0
        return 0.0
    
    def __setPivotColumns(self, configMetadata, learnedConfig):
        if configMetadata is None:
            return
        for config in configMetadata:
            column = config["ColumnName"]
            
            c = None
            if column in learnedConfig:
                c = learnedConfig[column]["Extensions"]
            elif "Extensions" in config and config["Extensions"] is not None:
                c = config["Extensions"]
            if c is None:
                continue

            pivotValues = None
            for e in c:
                if e["Key"] == "PivotValues":
                    pivotValues = e["Value"]
            if pivotValues is None:
                continue

            for pivotValue in pivotValues:
                pivotColumn = pivotValue["PivotColumn"]
                try:
                    pValues = self.columnsToPivot[pivotColumn]
                except KeyError:
                    pValues = (column, [])
                        
                pValues[1].append(pivotValue["PivotValue"])
                self.columnsToPivot[pivotColumn] = pValues


    def __writeRTSArtifact(self):
        if len(self.columnsToPivot) == 0:
            return
        with open("pivotvalues.txt", "w") as fp:
            fp.write(str(self.columnsToPivot))
            self.pivotValuesFilePath = os.path.abspath(fp.name)

    def getOutputColumns(self):
        return [(create_column(k, "INTEGER"), [v[0], k]) for k, v in self.columnsToPivot.items()]

    def getRTSMainModule(self):
        return "pivot"

    def getRTSArtifacts(self):
        return [("pivotvalues.txt", self.pivotValuesFilePath)]
    
    def doColumnCheck(self):
        return False

