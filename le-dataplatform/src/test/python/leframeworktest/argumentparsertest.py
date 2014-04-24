import leframework.argumentparser as ap
import unittest

class ArgumentParserTest(unittest.TestCase):

    def testCreateListForAvro(self):
        parser = ap.ArgumentParser("model.json")
        schema = parser.getSchema()
        training = parser.createList(schema["training_data"])
        self.assertEqual(training.shape, (2387, 8), "Dimensions of training data should be (2387, 7), but is " + str(training.shape))
        test = parser.createList(schema["test_data"])
        self.assertEqual(test.shape, (616, 8), "Dimensions of training data should be (616, 7), but is " + str(test.shape))
        algorithmProperties = parser.getAlgorithmProperties()
        self.assertTrue(algorithmProperties["C"] == "0.05")
        self.assertTrue(algorithmProperties["numestimators"] == "10")
        keyCols = parser.getKeyColumns()
        self.assertEquals(keyCols, (0,))

