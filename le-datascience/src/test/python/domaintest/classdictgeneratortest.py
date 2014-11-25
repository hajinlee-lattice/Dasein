'''
Created on Oct 23, 2014

@author: hliu
'''
import unittest
from domain.classdictgenerator import initDict

class ClassDictGeneratorTest(unittest.TestCase):

    def testInitDict(self):
        dicts = initDict("../../resources/jsonschema")
        self.assertEqual(len(dicts), 2)
        self.assertTrue(dicts.has_key("model_definition"))
        self.assertTrue(dicts.has_key("model"))
        self.assertTrue(dicts["model"].has_key("model_definition"))
        self.assertTrue(dicts["model_definition"].has_key("algorithms"))