import leframework.argumentparser as ap
import numpy

class AlgorithmTestBase(object):

    def execute(self, algorithmFileName, algorithmProperties):
        parser = ap.ArgumentParser("model.json")
        schema = parser.getSchema()
        training = parser.createList(schema["training_data"])
        test = parser.createList(schema["test_data"])
        modelDir = "./"
        schema["featureIndex"] = parser.getFeatureTuple()
        schema["targetIndex"] = parser.getTargetIndex()
        execfile("../../main/python/algorithm/" + algorithmFileName, globals())
        clf = globals()['train'](training, test, schema, modelDir, algorithmProperties)
        scored = clf.predict_proba(test[:, schema["featureIndex"]])[:,1]
        numpy.savetxt(modelDir + "scored.txt", scored, delimiter=",")
