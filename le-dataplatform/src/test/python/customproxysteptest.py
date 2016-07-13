import math
import os
import pandas as pd

import numpy as np
from trainingtestbase import TrainingTestBase


class CustomProxyStepTest(TrainingTestBase):

    def setUp(self):
        super(CustomProxyStepTest, self).setUp()
        os.symlink("pipelinetest/customtargetstep.py", "customtargetstep.py")
    
    def tearDown(self):
        super(CustomProxyStepTest, self).tearDown()
        
    def testCustomProxyStep(self):
        from customproxystep import CustomProxyStep
        schema = {'python_pipeline_lib' : 'lepipeline.tar.gz'}
        params = {'schema' : schema}
        
        step = CustomProxyStep(params, "customtargetstep", "CustomTargetStep", "/Pods/Default/Services/ModelQuality/customtargetstep.py")
        inputFrame = pd.DataFrame(columns=["InputColumn"])
        outputFrame = step.transform(inputFrame, {}, False)
        columns = list(outputFrame)
        print columns
        self.assertTrue('InputColumn' in columns)
        self.assertTrue('NewOutputColumn' in columns)
        
        
     