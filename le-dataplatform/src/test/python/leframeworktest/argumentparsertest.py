from testbase import TestBase
from leframework.argumentparser import ArgumentParser

class ArgumentParserTest(TestBase):

    def testCreateListForAvro(self):
        parser = ArgumentParser("model.json")
        schema = parser.getSchema()
        training = parser.createList(parser.stripPath(schema["training_data"]))
        self.assertEqual(training.shape, (6000, 111), "Dimensions of training data should be (6000, 111), but is " + str(training.shape))
        test = parser.createList(parser.stripPath(schema["test_data"]))
        self.assertEqual(test.shape, (2000, 111), "Dimensions of test data should be (2000, 111), but is " + str(test.shape))
        algorithmProperties = parser.getAlgorithmProperties()
        self.assertTrue(algorithmProperties["criterion"] == "gini")
        self.assertTrue(algorithmProperties["n_estimators"] == "100")
        self.assertEqual(len(parser.getKeys()), 1)

