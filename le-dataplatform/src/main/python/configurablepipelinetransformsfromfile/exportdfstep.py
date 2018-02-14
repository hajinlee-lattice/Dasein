'''
Description:

    This step will export the data frame up to this point in the pipeline to avro
'''
import os
import pandas as pd

from pipelinefwk import PipelineStep
from pipelinefwk import get_logger

PD_VERSION = pd.__version__
logger = get_logger("pipeline")

class ExportDataFrameStep(PipelineStep):

    def __init__(self): pass

    def transform(self, dataFrame, configMetadata, test):
        if test == False or "DEBUG" not in os.environ or os.environ["DEBUG"] != "true":
            return dataFrame

        columns = list(dataFrame.columns.values)
        columns = [x for x in columns if not x.startswith("###")]

        logger.info('Exporting these columns to exportdfstep.csv: [ "' + '", "'.join(columns) + '" ]')
        if int(PD_VERSION.split(".")[1]) < 14:
            dataFrame.to_csv("exportdfstep.csv", sep=',', encoding='utf-8', cols=columns, index=False, float_format='%.10f')
        else:
            dataFrame.to_csv("exportdfstep.csv", sep=',', encoding='utf-8', columns=columns, index=False, float_format='%.10f')
        return dataFrame
    
    def includeInScoringPipeline(self):
        return False
