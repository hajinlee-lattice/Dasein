import unittest

import leadlevelpredictor.schemagenerator as sgen

class SchemaGeneratorTest(unittest.TestCase):

    def testGenerate(self):
        schemagen = sgen.SchemaGenerator("/home/rgonzalez/Documents/Customers/MuleSoft/MulesoftProduction_6_29_14_updated.json",
                                         "sqlserverdatasource", "Leaf", "Lattice1", "MuleSoft_Analytics")
        schemagen.generate("P1_Event", "JakeHack_RemoveDuplicateDomains")