from leframework.model.states.modeldetailgenerator import ModelDetailGenerator
from testbase import TestBase
from leframework.model import mediator as mdtr


class ModelDetailGeneratorTest(TestBase):

    def setUp(self):
        self.sg = ModelDetailGenerator()

    def testExecute(self):
        self.assertEquals(self.sg.getName(), "ModelDetailGenerator")

    def testGetModelID(self):
        mediator = mdtr.Mediator()
        mediator.schema = {"name" : "PLSModel", "model_data_dir" : "/user/s-analytics/customers/Nutanix/models/Q_EventTable_Nutanix/58e6de15-5448-4009-a512-bd27d59ca75d"}
        self.sg.mediator = mediator
        self.assertEquals(self.sg.generateModelID(), "ms__58e6de15-5448-4009-a512-bd27d59ca75d-PLSModel")
