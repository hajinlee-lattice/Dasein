from trainingtestbase import TrainingTestBase
import numpy as np
import pandas as pd

from configurablepipelinetransformsfromfile.cleancategoricalcolumn import CleanCategoricalColumn

class CleanCategoricalColumnTransformTest(TrainingTestBase):

    def testConversionRate(self):
        self.checkCleanCategoricalColumn()

    def checkCleanCategoricalColumn(self):
        targetColumn = 'P1_Event'

        aa =    ['0']*10  +  ['1']*100  +  ['2']*100  +  ['3']*200  +  ['4']*2  +  ['5']*5  +  ['7']*300  +  ['8']*200  +  ['9']*5
        elist = [.1]*10   +  [.1]*100   +  [.1]*100   +  [.1]*200   +  [.8]*2   +  [1.0]*5  +  [.12]*300  +  [.08]*200  +  [0]*5

        trainingDataFrame = pd.DataFrame({ 
            'A' : aa,
            targetColumn : elist
        })

        copyOfTraining = trainingDataFrame.copy(deep=True)

        # We will only convert one column, the columnsNotToPivot is used to check if training/test separation is being done correctly
        columnsToPivot = {'A':1}
        columnsNotToPivot = {'B':1} # Not used in "training" phase. This column will be added to testing data frame
        cleanCategoricalColumn = CleanCategoricalColumn(columnsToPivot=columnsToPivot, targetColumn=targetColumn)
        cleanedColumns = cleanCategoricalColumn.transform(trainingDataFrame, configMetadata=None, test=None)

        includedKeys = cleanCategoricalColumn.getIncludedKeys()
        print includedKeys

        for index, convertedValue in cleanedColumns[columnsToPivot.keys()].itertuples():
            originalValueBeforeConversion = copyOfTraining['A'].iloc[index]
            if originalValueBeforeConversion not in includedKeys['A']:
                self.assertEqual(convertedValue, '0' , msg="Categorical Value not cleaned correctly")

        # In training mode, we do not re-learn the labels. 
        # Check this by generating a new dataset with different distribution and check that only the includedKeys get converted
        # Also generate a trainingColumn not in testing and make sure it is not converted
        
        aa =    ['0']*100  +  ['1']*100 + ['2']*100 + ['3']*2 + ['4']*2 + ['5']*5 + ['7']*3 + ['8']*200 + ['9']*5
        al =    ['a']*100 + ['b']*100 + ['c']*100 + ['d']*2 + ['e']*2 + ['f']*5 + ['g']*3 + ['h']*200 + ['i']*5
        elist = [.1]*100  + [.1]*100  + [.1]*100 +  [.1]*2 +  [.8]*2 +  [1.0]*5 + [.12]*3 + [.08]*200 + [0]*5

        testingDataFrame = pd.DataFrame({ 
            'A' : aa,
            'B' : al,
            targetColumn : elist
        })

        copyOfTesting = testingDataFrame.copy(deep=True)

        cleanedColumns = cleanCategoricalColumn.transform(testingDataFrame, configMetadata=None, test=None)

        trainingIncludedKeys = cleanCategoricalColumn.getIncludedKeys()

        self.assertTrue(trainingIncludedKeys, includedKeys)

        # Check that all learnt values in 'A'(column to pivot) get converted
        for index, convertedValue in cleanedColumns[columnsToPivot.keys()].itertuples():
            originalValueBeforeConversion = copyOfTesting['A'].iloc[index]
            if originalValueBeforeConversion not in includedKeys['A']:
                self.assertEqual(convertedValue, '0' , msg="Categorical Value not cleaned correctly")

        # Get the column that was not trained on and make sure it was not touched
        for index, unConvertedValue in cleanedColumns[columnsNotToPivot.keys()].itertuples():
            originalValueBeforeConversion = copyOfTesting['B'].iloc[index]
            self.assertEqual(unConvertedValue, originalValueBeforeConversion ,
                             msg="Categorical Value train/test logic not working correctly. Pre/Post clean value should be same")