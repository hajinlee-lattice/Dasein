from trainingtestbase import TrainingTestBase

class AggregatedModelTest(TrainingTestBase):

    def testExecuteLearning(self):
        from aggregatedmodel import AggregatedModel
        aggregatedModel = AggregatedModel()

        # Check if all pickles were loaded
        print "Models loaded by aggregatedmodeltest: " + str(len(aggregatedModel.models))
        self.assertTrue(len(aggregatedModel.models) > 0, "Pickle files were not loaded.")
        self.assertTrue(len(aggregatedModel.models[0].classes_) > 0, "No classes are detected by the random forest.")
