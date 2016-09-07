import math
import os
from pandas import DataFrame as df
import shutil
import sys

import numpy as np
from trainingtestbase import TrainingTestBase


class RevenueColumnTransformStepTest(TrainingTestBase):

    def setUp(self):
        super(RevenueColumnTransformStepTest, self).setUp()
    
    def tearDown(self):
        super(RevenueColumnTransformStepTest, self).tearDown()
        shutil.rmtree("./evpipeline.tar.gz", ignore_errors=True)
        
    def testEncodeNone(self):
        from evpipelinesteps import RevenueColumnTransformStep
        
        columnMapping = dict()
        columnMapping['Product_1_Revenue'] = 1
        columnMapping['Product_1_RevenueRollingSum6'] = 1
        columnMapping['Product_1_Units'] = 1
        columnMapping['Product_1_RevenueMomentum3'] = 1
        columnMapping['Target'] = 1

        step = RevenueColumnTransformStep(columnMapping)
        data = {'Product_1_Revenue' : [1, 0., np.NaN, -4.],
         'Product_1_RevenueRollingSum6' : [2., 3, 4., 5.],
         'Product_1_Units': [3, 4, np.NaN, -6],
         'Product_1_RevenueMomentum3': [4, 0, np.NaN, -7],
         'Target': [1, 1, 0, 0]
         }
        dataFrame = df(data)
        result = step.transform(dataFrame, None, False)
        columns = result.columns.tolist()
        self.assertEqual(len(columns), 5)
        self.assertTrue('Target' in columns)
        
        self.assertEqual(dataFrame['Product_1_Revenue'][0], 1.0)
        self.assertTrue(np.isnan(dataFrame['Product_1_Revenue'][1]))
        self.assertTrue(np.isnan(dataFrame['Product_1_Revenue'][2]), math.log(2))
        self.assertTrue(np.isnan(dataFrame['Product_1_Revenue'][3]))

        self.assertEqual(dataFrame['Product_1_RevenueMomentum3'][0], 4)
        self.assertTrue(np.isnan(dataFrame['Product_1_RevenueMomentum3'][1]))
        self.assertTrue(np.isnan(dataFrame['Product_1_RevenueMomentum3'][2]))
        self.assertEqual(dataFrame['Product_1_RevenueMomentum3'][3], -7)