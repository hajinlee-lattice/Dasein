from dataruletestbase import DataRuleTestBase
from datarules.dataruleutilsds import convertCleanDataFrame
from pandas.core.frame import DataFrame
import numpy as np
import sys
import io
from StringIO import StringIO

class DataRuleUtilsDSTest(DataRuleTestBase):

    def testConvertDataFrame(self):
        # Redirect Stdout and capture it, then check to make sure no Exception is thrown
        saved_stdout = sys.stdout
        out = StringIO()
        try:
            sys.stdout = out

            self.callConvertDataFrame()

            output = out.getvalue().strip()
            print output.find("Exception")
            assert output.find("Exception") == -1
        finally:
            sys.stdout = saved_stdout

    def callConvertDataFrame(self):
        dataFrame = DataFrame({
                       u'LeadID': range(1, 5),
                       u'Domain': ['gmail.com', 'b', 'c', 'c'],
                       u'TestColumn': [None, np.NaN, 1, 2],
                       u'P1_Event': np.random.binomial(5, 0.2)
                      })

        dataFrame['TestColumn'][2:3] = None

        result = convertCleanDataFrame(dataFrame.columns, dataFrame, set(['Domain']), set(['LeadID', 'P1_Event', 'TestColumn']))



