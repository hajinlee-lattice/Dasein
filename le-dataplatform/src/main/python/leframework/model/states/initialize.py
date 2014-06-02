import fastavro as avro

from leframework.codestyle import overrides
from leframework.model.state import State


class Initialize(State):
    
    def __init__(self):
        State.__init__(self, "Initialize")
    
    @overrides(State)
    def execute(self):
        mediator = self.getMediator()
        scored = self.score(mediator)
        metadata = self.retrieveMetadata(mediator)
        mediator.scored = scored
        mediator.metadata = metadata
        
    def score(self, mediator):
        scored = mediator.clf.predict_proba(mediator.data[:, mediator.schema["featureIndex"]])
        return scored
    
    def retrieveMetadata(self, mediator):
        metadata = dict()
        with open(mediator.schema["metadata"]) as fp:
            reader = avro.reader(fp)
            for record in reader:
                colname = record["barecolumnname"]
                
                if colname in metadata:
                    metadata[colname].append(record)
                else:
                    metadata[colname] = [record]
        return metadata