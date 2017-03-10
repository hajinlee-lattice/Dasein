
import os, sys, shutil
from trainingtestbase import TrainingTestBase
import logging
from unittest import TestCase
import pandas as pd
import numpy as np

from leframework.util.precisionutil import PrecisionUtil

logging.basicConfig(level=logging.INFO, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='precisiontest')

class PrecisionTest(TestCase):

    @classmethod
    def setUpClass(cls):
        logger.info("=========Current test: " + str(cls) + " ===========")
        #cls.basePath = os.path.join('..','..') if os.path.exists(os.path.join('..','..','main','python','rulefwk.py')) else os.path.join('..','..','..')

    @classmethod
    def tearDownClass(cls):
        logger.info("=========Tear down test: " + str(cls) + " ===========")

    def testPrecisionUtil(self):

        valuesToTest = [ \
            np.float64(3.14159265358979323846264338327950), \
            np.float64(-3.14159265358979323846264338327950), \
            np.float64(2.71828182846e15), \
            np.float64(-2.71828182846e15), \
            np.float64(2.71828182846e-15), \
            0.00057615364, \
            24756, \
            0.3333333333333, \
            3.33e-1 \
        ]

        valuesAtPrecision3 = [3.14, -3.14, 2.72e15, -2.72e15, 2.72e-15, 5.76e-4, 2.48e4, 0.333, 0.333]
        print valuesAtPrecision3

        vs = pd.DataFrame(data=[{'value':v} for v in valuesToTest])
        vss = vs[['value']].apply(lambda x : PrecisionUtil.setPrecision(x, 3))

        for i in range(0,len(valuesAtPrecision3)):
            self.assertEquals(vss['value'][i], valuesAtPrecision3[i])

        ## Integer values should not be affected
        intValue = 12345678910111213
        self.assertEquals(PrecisionUtil.setPrecision(intValue, 3), intValue)
