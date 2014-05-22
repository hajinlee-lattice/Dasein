import unittest

import leadlevelpredictor.schemagenerator as sgen

class SchemaGeneratorTest(unittest.TestCase):

    def testGenerate(self):
        schemagen = sgen.SchemaGenerator("/home/rgonzalez/Documents/Customers/Nutanix/Nutanix.json",
                                         "sqlserverdatasource", "DataLoader_Dep", "L@ttice1", "EventTableTestingDB")
        schemagen.generate("P1_Event", "Nutanix_EventTable")