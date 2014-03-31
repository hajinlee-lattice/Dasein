import leframework.argumentparser as ap
import unittest

class ArgumentParserTest(unittest.TestCase):

    def testCreateListForAvro(self):
        parser = ap.ArgumentParser("model.json")
        schema = parser.getSchema()
        training = parser.createList(schema["training_data"])
        self.assertEqual(training.shape, (2387, 7), "Dimensions of training data should be (2387, 7)")
        test = parser.createList(schema["test_data"])
        self.assertEqual(test.shape, (616, 7), "Dimensions of training data should be (616, 7)")

