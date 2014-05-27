import numpy
from leframework.codestyle import overrides
from leframework.model.state import State

class Finalize(State):

    def __init__(self):
        State.__init__(self, "Finalize")
    
    @overrides(State)
    def execute(self):
        mediator = self.getMediator()
        scored = mediator.scored
        # add the key data and append the scored data
        scored = numpy.apply_along_axis(lambda x: [x[1]], 1, scored)
        keyData = mediator.data[:, mediator.schema["keyColIndex"]]
        scored = numpy.concatenate((keyData, scored), axis=1)
        # write the scored data to file
        numpy.savetxt(mediator.modelLocalDir + "scored.txt", scored, delimiter=",")

