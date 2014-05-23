import unittest
import leframework.model.states.summarygenerator as summarygen

class SummaryGeneratorTest(unittest.TestCase):

    def testExecute(self):
        summaryGenerator = summarygen.SummaryGenerator()
        self.assertEquals(summaryGenerator.getName(), "SummaryGenerator")
        self.assertEquals(summaryGenerator.getKey(), "Summary")
