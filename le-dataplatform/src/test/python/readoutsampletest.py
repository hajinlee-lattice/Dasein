import numpy
import os
import pandas
import sys

from trainingtestbase import TrainingTestBase


class ReadoutSampleTest(TrainingTestBase):

    def tearDown(self):
        super(TrainingTestBase, self).tearDown()
        self.tearDownClass()
        self.setUpClass()

    def testReadoutSample(self):
        self.launch("model.json")
        self.checkResults(expectedRows = 2000)

    def testReadoutSampleReadouts(self):
        self.launch("model-readouts.json")
        self.checkResults(expectedRows = 2000)

    def testReadoutSampleCSV(self):
        self.launch("model-csv.json")
        self.checkResults(expectedRows = 2000)
 
    def testReadoutSampleLegacy(self):
        self.launch("model-legacy.json")
        self.checkResults(expectedRows = 2000, includeReadouts = False)

    def launch(self, model):
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules: del sys.modules['launcher']
        from launcher import Launcher
        launcher = Launcher(model)
        launcher.execute(False)
        launcher.training

    def checkResults(self, expectedRows, includeReadouts = True):
        targetColumnName = "P1_Event"
        scoreColumnName = "Score"
        scoreColumnIndex = 0
        convertedColumName = "Converted"
        convertedColumIndex = 1
        leadIDColumnName = "LeadID"
        leadIDColumnIndex = 2
        emailColumName = "Email"
        emailColumIndex = 3

        # Output File Exists?
        outputFile = "./results/readoutsample.csv"
        self.assertTrue(os.path.isfile(outputFile))

        # Construct DataFrame?
        dataFrame = pandas.read_csv(outputFile)
        self.assertIsNotNone(dataFrame)

        # Number of Rows as Expected?
        (rows, _) = dataFrame.shape
        self.assertEqual(expectedRows, rows)

        # Score/Converted/Readout Columns as Expected?
        columns = dataFrame.columns.tolist()
        self.assertEqual(columns[scoreColumnIndex], scoreColumnName)
        self.assertEqual(columns[convertedColumIndex], convertedColumName)
        self.assertEqual(dataFrame.dtypes[scoreColumnName], numpy.float64)
        self.assertEqual(dataFrame.dtypes[convertedColumIndex], numpy.object)
        if includeReadouts:
            self.assertEqual(columns[leadIDColumnIndex], leadIDColumnName)
            self.assertEqual(columns[emailColumIndex], emailColumName)
            self.assertEqual(dataFrame.dtypes[leadIDColumnName], numpy.float64)
            self.assertEqual(dataFrame.dtypes[emailColumName], numpy.object)

        # Score Range as Expected?
        for value in dataFrame[scoreColumnName].as_matrix():
            self.assertLessEqual(value, 1)
            self.assertGreaterEqual(value, 0)

        # Converted Range as Expected?
        for value in dataFrame[convertedColumName].as_matrix():
            self.assertTrue(value == "N" or value == "Y")

        # Target Range as Expected?
        for value in dataFrame[targetColumnName].as_matrix():
            self.assertLessEqual(value, 1)
            self.assertGreaterEqual(value, 0)

        # Converted as Expected?
        for converted, target in zip(dataFrame[convertedColumName].as_matrix(), dataFrame[targetColumnName].as_matrix()):
            self.assertEqual(converted, "Y" if target > 0 else "N")

        # Sorted as Expected?
        dataFrameCopy = dataFrame.copy()
        dataFrameCopy.sort(scoreColumnName, axis = 0, ascending = False, inplace = True)
        for value, copyValue in zip(dataFrame[scoreColumnName].as_matrix(), dataFrameCopy[scoreColumnName].as_matrix()):
            self.assertEqual(value, copyValue)
