import leframework.argumentparser as ap
import launcher as l
import os


class AlgorithmTestBase(object):
    
    def execute(self, algorithmFileName, algorithmProperties):
        parser = ap.ArgumentParser("model.json")
        schema = parser.getSchema()
        training = parser.createList(schema["training_data"])
        test = parser.createList(schema["test_data"])
        modelDir = "./results/"
        os.mkdir(modelDir)
        launcher = l.Launcher("model.json")
        launcher.populateSchemaWithMetadata(schema, parser)
        execfile("../../main/python/algorithm/" + algorithmFileName, globals())
        return globals()['train'](training, test, schema, modelDir, algorithmProperties)
        
        
