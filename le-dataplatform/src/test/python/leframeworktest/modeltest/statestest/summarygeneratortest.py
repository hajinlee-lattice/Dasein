from leframework.model.states.summarygenerator import SummaryGenerator
from testbase import TestBase


class SummaryGeneratorTest(TestBase):
    
    def setUp(self):
        self.sg = SummaryGenerator()

    def testExecute(self):
        self.assertEquals(self.sg.getName(), "SummaryGenerator")
        self.assertEquals(self.sg.getKey(), "Summary")
