'''
Description:

    This step will export the data frame up to this point in the pipeline to avro
'''
from pipelinefwk import PipelineStep
from pipelinefwk import get_logger


logger = get_logger("pipeline")

class ExportDataFrameStep(PipelineStep):
    
    def __init__(self): pass

    def transform(self, dataFrame, configMetadata, test):
        if configMetadata is not None or test == False:
            return dataFrame
        
        columns = list(dataFrame.columns.values)
        columns = [x for x in columns if not x.startswith("###")]
        dataFrame.to_csv("exportdfstep_%s.csv" % str(test), sep=',', encoding='utf-8', columns=columns)
        return dataFrame