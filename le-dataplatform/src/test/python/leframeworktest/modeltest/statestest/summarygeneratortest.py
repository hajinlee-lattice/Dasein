from testbase import TestBase

from leframework.model.states.summarygenerator import SummaryGenerator

class SummaryGeneratorTest(TestBase):

    def testExecute(self):
        summaryGenerator = SummaryGenerator()
        self.assertEquals(summaryGenerator.getName(), "SummaryGenerator")
        self.assertEquals(summaryGenerator.getKey(), "Summary")
