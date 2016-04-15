'''
Description:

    This step will pivot the list of columns by retrieving the metadata from our config metadata structure
'''
import os

from pipelinefwk import PipelineStep
from pipelinefwk import create_column
from pipelinefwk import get_logger


logger = get_logger("pipeline")

class PivotStep(PipelineStep):
    
    columnsToPivot = {}
    pivotValuesFilePath = None
    
    def __init__(self, columnsToPivot={}):
        self.columnsToPivot = columnsToPivot
        
    def learnParameters(self, trainingDataFrame, testDataFrame, configMetadata):
        if configMetadata is not None and len(self.columnsToPivot) == 0:
            self.__setPivotColumns(configMetadata)
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
    
    def __setPivotColumns(self, configMetadata):
        for config in configMetadata:
            column = config["ColumnName"]
            if "Extensions" in config and config["Extensions"] is not None:
                pivotValues = None
                for e in config["Extensions"]:
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

