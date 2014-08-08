import os
from encoder import HashEncoder
from leframework.argumentparser import ArgumentParser
from pipeline import EnumeratedColumnTransformStep
from pipeline import ImputationStep
from pipeline import Pipeline


class AlgorithmTestBase(object):
    
    def getDecoratedColumns(self, parser):
        stringColumns = parser.getStringColumns()
        allFeatures = parser.getNameToFeatureIndex()
        continuousColumns = set(allFeatures.keys()) - stringColumns
        stringCols = dict()
        transform = HashEncoder()
        for stringColumn in stringColumns:
            stringCols[stringColumn] = transform
        continuousCols = dict()
        for continuousCol in continuousColumns:
            continuousCols[continuousCol] = 0.0
        return (stringCols, continuousCols)
    
    def execute(self, algorithmFileName, algorithmProperties, doTransformation=True):
        parser = ArgumentParser("model.json")
        schema = parser.getSchema()
        training = parser.createList(parser.stripPath(schema["training_data"]))
        test = parser.createList(parser.stripPath(schema["test_data"]))
        modelDir = "./results/"
        os.mkdir(modelDir)
        execfile("../../main/python/algorithm/" + algorithmFileName, globals())
        
        if doTransformation:
            (stringCols, continuousCols) = self.getDecoratedColumns(parser)
            
            steps = [EnumeratedColumnTransformStep(stringCols), ImputationStep(continuousCols)]
            pipeline = Pipeline(steps)
            training = pipeline.predict(training)
            test = pipeline.predict(test)

        return globals()['train'](training, test, schema, modelDir, algorithmProperties)
        
        
