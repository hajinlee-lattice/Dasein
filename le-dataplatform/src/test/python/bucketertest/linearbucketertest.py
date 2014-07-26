from bucketertestbase import BucketerTestBase
from unittest import TestCase
import numpy as np

class LinearBucketerTest(TestCase, BucketerTestBase):

    def setUp(self):
        self.setup()
        
    def testBucketing(self): 
        params = {'maxBuckets': 8, 'minValue': 10}
        bandsList = self.bucketColumns('linear', params)
        for bands in bandsList:
            self.assertTrue(len(bands) <= params['maxBuckets']+1)
            for band in bands:
                if band is not None:
                    self.assertFalse(np.isinf(band))

