from unittest import TestCase

from leframework.model.states.summarygenerator import SummaryGenerator

class SummaryGeneratorTest(TestCase):

    def testExecute(self):
        summaryGenerator = SummaryGenerator()
        self.assertEquals(summaryGenerator.getName(), "SummaryGenerator")
        self.assertEquals(summaryGenerator.getKey(), "Summary")
