import os
from leframework.argumentparser import ArgumentParser


class AlgorithmTestBase(object):
    
    def execute(self, algorithmFileName, algorithmProperties):
        parser = ArgumentParser("model.json")
        schema = parser.getSchema()
        training = parser.createList(schema["training_data"])
        test = parser.createList(schema["test_data"])
        modelDir = "./results/"
        os.mkdir(modelDir)
        execfile("../../main/python/algorithm/" + algorithmFileName, globals())
        return globals()['train'](training, test, schema, modelDir, algorithmProperties)
        
        
