import leframework.argumentparser as ap

class AlgorithmBaseTest(object):

    def execute(self, algorithmFileName):
        parser = ap.ArgumentParser("model.json")
        schema = parser.getSchema()
        training = parser.createList(schema["training_data"])
        test = parser.createList(schema["test_data"])
        modelDir = "./"
        schema["featureIndex"] = parser.getFeatureTuple()
        schema["targetIndex"] = parser.getTargetIndex()
        execfile("../../../main/python/algorithm/" + algorithmFileName, globals())
        globals()['train'](training, test, schema, modelDir)
