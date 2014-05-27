from leframework.argumentparser import ArgumentParser
from launcher import Launcher 
import os


class AlgorithmTestBase(object):
    
    def execute(self, algorithmFileName, algorithmProperties):
        parser = ArgumentParser("model.json")
        schema = parser.getSchema()
        training = parser.createList(schema["training_data"])
        test = parser.createList(schema["test_data"])
        modelDir = "./results/"
        os.mkdir(modelDir)
        launcher = Launcher("model.json")
        launcher.populateSchemaWithMetadata(schema, parser)
        execfile("../../main/python/algorithm/" + algorithmFileName, globals())
        return globals()['train'](training, test, schema, modelDir, algorithmProperties)
        
        
