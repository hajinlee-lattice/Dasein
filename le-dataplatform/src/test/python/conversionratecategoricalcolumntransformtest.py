from trainingtestbase import TrainingTestBase
import numpy as np
import pandas as pd

from configurablepipelinetransformsfromfile.assignconversionratetocategoricalcolumns import AssignConversionRateToCategoricalColumns

class ConversionRateCategoricalColumnTransformTest(TrainingTestBase):

    def testConversionRate(self):
        self.checkConversionRateAssignment()

    def checkConversionRateAssignment(self):
        trainingSize = 5000
        categoriesAndWeightages = {"Apple" : 0.23,
                                   "Oranges" : 0.27,
                                   "Kiwis": 0.1,
                                   "Dragon Fruit": 0.4}

        targetColumn = 'P1_Event'
        conversionRate = 0.7

        trainingDataFrame = pd.DataFrame({ 
            'A' : pd.Series(np.random.uniform(0, 1, size = trainingSize)),
            'B' : pd.Series(1,index=list(range(trainingSize)),dtype='float32'),
            'C' : pd.Series(1,index=list(range(trainingSize)),dtype='float32'),
            'D' : pd.Series(np.array([3] * trainingSize,dtype='int32')),
            'E' : pd.Categorical(np.random.choice(categoriesAndWeightages.keys(), trainingSize, p=categoriesAndWeightages.values())),
            targetColumn : pd.Series(np.random.binomial(n=1, p=conversionRate, size = trainingSize))
        })

        copyOfTraining = trainingDataFrame.copy(deep=True)

        # Only Convert one column, we will use the other one to check the train/test logic
        columnsToPivot = {'E':1}
        columnsNotToPivot = {'F':1} # Not used in "training" phase. This column will be added to testing data frame
        conversionRateAssigner = AssignConversionRateToCategoricalColumns(columnsToPivot=columnsToPivot, targetColumn=targetColumn)

        convertedDF = conversionRateAssigner.transform(trainingDataFrame, configMetadata=None, test=None)
        columnMapping = conversionRateAssigner.getConversionKey()
        for index, convertedValue in convertedDF[columnsToPivot.keys()].itertuples():
            originalValueBeforeConversion = columnMapping['E'][copyOfTraining['E'].iloc[index]]
            self.assertAlmostEqual(float(convertedValue), originalValueBeforeConversion , 
                                   msg="Categorical Value not assigned correctly", delta=0.01)

        # Check testing phase, change the distribution of the categorical labels, but we should see the same mapping.
        testingSize = 2000
        testingConversionRate = 0.3
        testingDataFrame = pd.DataFrame({ 
            'A' : pd.Series(np.random.uniform(0, 1, size = testingSize)),
            'B' : pd.Series(1,index=list(range(testingSize)),dtype='float32'),
            'C' : pd.Series(1,index=list(range(testingSize)),dtype='float32'),
            'D' : pd.Series(np.array([3] * testingSize,dtype='int32')),
            'E' : pd.Categorical(np.random.choice(categoriesAndWeightages.keys(), testingSize, p=categoriesAndWeightages.values())),
            'F' : pd.Categorical(np.random.choice(categoriesAndWeightages.keys(), testingSize, p=categoriesAndWeightages.values())),
            targetColumn : pd.Series(np.random.binomial(n=1, p=testingConversionRate, size = testingSize))
        })
        copyOfTesting = testingDataFrame.copy(deep=True)

        testingConvertedDF = conversionRateAssigner.transform(testingDataFrame, configMetadata=None, test=None)

        for index, convertedValue in testingConvertedDF[columnsToPivot.keys()].itertuples():
            originalValueBeforeConversionTesting = columnMapping['E'][copyOfTesting['E'].iloc[index]]
            self.assertAlmostEqual(float(convertedValue), originalValueBeforeConversionTesting, 
                                   msg="Categorical Value not assigned correctly", delta=0.01)

        # Get the column that was not trained on and make sure it was not touched
        for index, unConvertedValue in testingConvertedDF[columnsNotToPivot.keys()].itertuples():
            originalValueBeforeConversionTesting = copyOfTesting['F'].iloc[index]
            self.assertEqual(unConvertedValue, originalValueBeforeConversionTesting, 
                                   msg="Conversion Rate Assignment train/test logic not working correctly. Pre/Post clean value should be same")
