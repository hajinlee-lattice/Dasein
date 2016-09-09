from leframework.argumentparser import ArgumentParser
from leframework.bucketers.bucketerdispatcher import BucketerDispatcher


class BucketerTestBase(object):

    def setup(self):
        testSize = 100
        parser = ArgumentParser("model-dataprofile.json")
        schema = parser.getSchema()
        training = parser.createList(parser.stripPath(schema["training_data"]))
        self.data = training[:testSize]
        self.target = self.data[schema["target"]]
        self.stringcols = set(schema["stringColumns"])
        self.features = set(schema["features"])
        self.dispatcher = BucketerDispatcher()
        
    def bucketColumns(self, methodType, methodParams):
        bandsList = []
        colnames = list(self.data.columns.values)
        for colname in colnames:
            print colname
            if colname in self.features and colname not in self.stringcols:
                b = self.dispatcher.bucketColumn(self.data[colname], self.target, methodType, methodParams)
                print(b)
                bandsList.append(b)
        
        return bandsList


