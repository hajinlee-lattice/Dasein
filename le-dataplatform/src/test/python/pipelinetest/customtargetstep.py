'''
Description:

    This is a sample custom target step into the pipeline by adding new column
'''
import os
from pipelinefwk import PipelineStep
from pipelinefwk import get_logger


logger = get_logger("pipeline")

class CustomTargetStep(PipelineStep):

    def __init__(self, params):
        self.params = params

    def transform(self, dataFrame, configMetadata, test):
        logger.info('Starting to run CustomTargetStep.')
        
        dataFrame["NewOutputColumn"] = 0
        
        logger.info('Finished Custom Target Step.')
        return dataFrame
