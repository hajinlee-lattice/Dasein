from leframework.model import mediator as mdtr
from leframework.progressreporter import ProgressReporter

class StateMachine(object):
    
    def __init__(self, amHost = None, amPort = 0):
        self.mediator = mdtr.Mediator()
        self.progressReporter = ProgressReporter(amHost, amPort)
        self.states = []
        
    def addState(self, state, jsonOrder):
        state.setMediator(self.mediator)
        state.setStateMachine(self)
        state.setJsonOrder(jsonOrder)
        self.states.append(state)
        
    def getStates(self):
        return sorted(self.states, key = lambda state: state.jsonOrder)
        
    def run(self):
        self.progressReporter.setTotalState(len(self.states))
        # Finished data preprocessing step
        self.progressReporter.inStateMachine()
        for state in self.states:
            state.execute()
            self.progressReporter.nextState()
    
    def getMediator(self):
        return self.mediator
