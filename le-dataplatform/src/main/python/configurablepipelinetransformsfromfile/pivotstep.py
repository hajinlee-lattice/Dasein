from pipelinefwk import get_logger
from pipelinefwk import create_column
from pipelinefwk import PipelineStep

'''
Description:

    This step will pivot the list of columns by retrieving the metadata from our config metadata structure
'''

logger = get_logger("pipeline")

class PivotStep(PipelineStep):
    
    columnsToPivot = {}
    columnList = {}
    columnValues = {}
    
    def __init__(self, columnsToPivot={}):
        self.columnsToPivot = columnsToPivot
        if columnsToPivot:
            for columnName, columnValues in columnsToPivot.items():
                for columnValue in columnValues:
                    self.columnList.put("%s%s" % (columnName, columnValue), columnName)
                    self.columnValues.put("%s%s" % (columnName, columnValue), columnValue)

    def transform(self, dataFrame, configMetadata, test):
        if configMetadata is not None:
            self.__setPivotColumns(configMetadata)
        for k, v in self.columnList.items():
            value = self.columnValues.get(k)
            dataFrame[k] = dataFrame[v].apply(lambda row: 1 if row == value else 0)
        return dataFrame
    
    def __setPivotColumns(self, configMetadata):
        pass

    def getOutputColumns(self):
        return [(create_column(k, "INT"), [v]) for k, v in self.columnList.items()]

    def getRTSMainModule(self):
        return "pivot"
