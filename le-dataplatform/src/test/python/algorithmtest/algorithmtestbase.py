import leframework.argumentparser as ap
import numpy as np
import launcher as l

class AlgorithmTestBase(object):

    def execute(self, algorithmFileName, algorithmProperties):
        parser = ap.ArgumentParser("model.json")
        schema = parser.getSchema()
        training = parser.createList(schema["training_data"])
        test = parser.createList(schema["test_data"])
        modelDir = "./"
        l.populateSchemaWithMetadata(schema, parser)
        execfile("../../main/python/algorithm/" + algorithmFileName, globals())
        clf = globals()['train'](training, test, schema, modelDir, algorithmProperties)
        l.writeScoredTestData(test, schema, clf, modelDir)
