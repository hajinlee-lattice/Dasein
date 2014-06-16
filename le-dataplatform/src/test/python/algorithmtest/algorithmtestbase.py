import os
from leframework.argumentparser import ArgumentParser
from pipeline import EnumeratedColumnTransformStep
from pipeline import ImputationStep
from pipeline import Pipeline


class AlgorithmTestBase(object):
    
    def execute(self, algorithmFileName, algorithmProperties, doTransformation=True):
        parser = ArgumentParser("model.json")
        schema = parser.getSchema()
        training = parser.createList(parser.stripPath(schema["training_data"]))
        test = parser.createList(parser.stripPath(schema["test_data"]))
        modelDir = "./results/"
        os.mkdir(modelDir)
        execfile("../../main/python/algorithm/" + algorithmFileName, globals())
        
        if doTransformation:
            steps = [EnumeratedColumnTransformStep(parser.getStringColumns()), ImputationStep()]
            pipeline = Pipeline(steps)
            training = pipeline.predict(training).as_matrix()
            test = pipeline.predict(test).as_matrix()

        return globals()['train'](training, test, schema, modelDir, algorithmProperties)
        
        
