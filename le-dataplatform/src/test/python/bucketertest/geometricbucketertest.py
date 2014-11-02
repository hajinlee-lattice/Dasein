from bucketertestbase import BucketerTestBase
from leframework.bucketers.geometricbucketer import GeometricBucketer
import numpy as np
import pandas as pd
from testbase import TestBase

class GeometricBucketerTest(TestBase, BucketerTestBase):

    def setUp(self):
        self.bucketer = GeometricBucketer()
        self.positiveValues = [1, 2, 5, 10, 20, 20, 20, 50, 50, 70, 80, 1500, 5000]
        self.bothValues = self.positiveValues + map(lambda x: -x, self.positiveValues)

    def testPositiveValues(self):
        binResults = self.bucketer.generateGeometricBins(pd.Series(self.positiveValues), 1, [2, 2.5, 2], minSamples=1)
        self.assertEqual(binResults, [0, 2, 5.0, 10.0, 20.0, 50.0, 100.0, 2000.0, np.inf])
     
    def testOnlyPositivesWithNegativeBins(self):
        binResults = self.bucketer.generateGeometricBins(pd.Series(self.positiveValues), 1, [2, 2.5, 2], minSamples=1, includeNegatives=True)
        self.assertEqual(binResults, [-np.inf, 0, 2, 5.0, 10.0, 20.0, 50.0, 100.0, 2000.0, np.inf])
        
    def testBothValuesNoNegativeBins(self):
        binResults = self.bucketer.generateGeometricBins(pd.Series(self.bothValues), 1, [2, 2.5, 2], minSamples=1)
        self.assertEqual(binResults, [0, 2, 5.0, 10.0, 20.0, 50.0, 100.0, 2000.0, np.inf])
        
    def testBothValuesWithNegativeBins(self):
        binResults = self.bucketer.generateGeometricBins(pd.Series(self.bothValues), 1, [2, 2.5, 2], minSamples=1, includeNegatives=True)
        self.assertEqual(binResults, [-np.inf, -2000.0, -100.0, -50.0, -20.0, -10.0, -5.0, -2, 0, 2, 5.0, 10.0, 20.0, 50.0, 100.0, 2000.0, np.inf])

