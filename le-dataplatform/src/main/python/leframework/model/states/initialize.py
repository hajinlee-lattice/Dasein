import logging

import fastavro as avro
from leframework.codestyle import overrides
from leframework.model.state import State
import encoder

class Initialize(State):
    
    def __init__(self):
        State.__init__(self, "Initialize")
        self.logger = logging.getLogger(name='initialize')
    
    @overrides(State)
    def execute(self):
        mediator = self.getMediator()
        scored = self.score(mediator)
        metadata = self.retrieveMetadata(mediator)
        mediator.scored = scored
        mediator.metadata = metadata
        
    def score(self, mediator):
        scored = mediator.clf.predict_proba(mediator.data[:, mediator.schema["featureIndex"]])
        scored = [row[1] for row in scored]
        return scored
    
    def retrieveMetadata(self, mediator):
        metadata = dict()
        realColNameToRecord = dict()
        with open(mediator.schema["metadata"]) as fp:
            reader = avro.reader(fp)
            for record in reader:
                colname = record["barecolumnname"]
                sqlcolname = ""
                record["hashValue"] = None
                if record["Dtype"] == "BND":
                    sqlcolname = colname + "_Continuous"  if self.mediator.depivoted else colname
                elif self.mediator.depivoted:
                    sqlcolname = colname + "_" + record["columnvalue"]
                else:
                    sqlcolname = colname
                    record["hashValue"] = encoder.transform(record["columnvalue"])
                
                if colname in metadata:
                    metadata[colname].append(record)                  
                else:
                    metadata[colname] = [record]
                
                realColNameToRecord[sqlcolname] = [record]
        return (metadata, realColNameToRecord)