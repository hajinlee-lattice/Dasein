import unittest

import leadlevelpredictor.schemagenerator as sgen

class SchemaGeneratorTest(unittest.TestCase):

    def testGenerate(self):
        schemagen = sgen.SchemaGenerator("/home/rgonzalez/Downloads/FireEye_Json_20140925.json",
                                         "sqlserverdatasource", "Leaf", "Lattice1", "FireEye_Analytics")
        schemagen.generate("P1_Event", "FireEye_EventTable")