from leframework.bucketers.bucketerdispatcher import BucketerDispatcher
from leframework.argumentparser import ArgumentParser


class BucketerTestBase(object):

    def setup(self):
        testSize = 100
        parser = ArgumentParser("model-dataprofile.json")
        schema = parser.getSchema()
        (training, _) = parser.createList(parser.stripPath(schema["training_data"]))
        self.data = training[:testSize]
        self.target = self.data.iloc[:,schema["targetIndex"]]
        self.stringcols = set(schema["stringColumns"])
        self.features = set(schema["features"])
        self.dispatcher = BucketerDispatcher()
        
    def bucketColumns(self, methodType, methodParams):
        bandsList = []
        colnames = list(self.data.columns.values)
        for colname in colnames:
            if colname in self.features and colname not in self.stringcols:
                bandsList.append(self.dispatcher.bucketColumn(self.data[colname], self.target, methodType, methodParams))
        
        return bandsList


