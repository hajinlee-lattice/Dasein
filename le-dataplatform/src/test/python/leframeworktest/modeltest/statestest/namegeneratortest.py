from leframework.model.states.namegenerator import NameGenerator
from leframework.model import mediator as mdtr
from testbase import TestBase

class NameGeneratorTest(TestBase):

    def setUp(self):
        self.ng = NameGenerator()

    def testExecute(self):
        self.assertEquals(self.ng.getName(), "NameGenerator")
        self.assertEquals(self.ng.getKey(), "Name")
        mediator = mdtr.Mediator()
        mediator.schema = {"name" : "PLSModel"}

        mediator.provenanceProperties = {}
        self.ng.mediator = mediator
        self.ng.execute()
        name = self.ng.mediator.name
        self.assertEquals(name.find("PLSModel"), 0)

        mediator.provenanceProperties = {"DataLoader_TenantName" : "DLTenant"}
        self.ng.mediator = mediator
        self.ng.execute()
        name = self.ng.mediator.name
        self.assertEquals(name.find("DLTenant_PLSModel"), 0)
        