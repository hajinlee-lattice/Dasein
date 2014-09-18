from leframework.model.states.summarygenerator import SummaryGenerator
from testbase import TestBase


class SummaryGeneratorTest(TestBase):

    def testExecute(self):
        summaryGenerator = SummaryGenerator()
        self.assertEquals(summaryGenerator.getName(), "SummaryGenerator")
        self.assertEquals(summaryGenerator.getKey(), "Summary")
        
    def testGenerateRocScore(self):
        score = [(0.5, 1),(0.5, 0),(0.5, 1),(0.5, 0),(0.5, 1),(0.5, 0)]
        summaryGenerator = SummaryGenerator()
        rocScore = summaryGenerator.getRocScore(score)
        self.assertEquals(rocScore, 0.7)
     
