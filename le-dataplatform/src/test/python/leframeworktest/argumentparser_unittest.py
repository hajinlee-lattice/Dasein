from testbase import TestBase
from leframework.argumentparser import ArgumentParser

class ArgumentParserTest(TestBase):

    def testCreateListForAvro(self):
        parser = ArgumentParser("model.json")
        schema = parser.getSchema()
        training = parser.createList(parser.stripPath(schema["training_data"]))
        self.assertEqual(training.shape, (17040, 102), "Dimensions of training data should be (17040, 102), but is " + str(training.shape))
        test = parser.createList(parser.stripPath(schema["test_data"]))
        self.assertEqual(test.shape, (4294, 102), "Dimensions of training data should be (4294, 102), but is " + str(test.shape))
        algorithmProperties = parser.getAlgorithmProperties()
        self.assertTrue(algorithmProperties["criterion"] == "gini")
        self.assertTrue(algorithmProperties["n_estimators"] == "10")
        keyCols = parser.getKeyColumns()
        self.assertEquals(keyCols, (0,))

