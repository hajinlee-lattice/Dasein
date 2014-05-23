from leframework.model import mediator as mdtr

class StateMachine(object):
    
    def __init__(self):
        self.mediator = mdtr.Mediator()
        self.states = []
        
    def addState(self, state):
        state.setMediator(self.mediator)
        self.states.append(state)
        
    def getStates(self):
        return self.states
        
    def run(self):
        for state in self.states:
            state.execute()
    
    def getMediator(self):
        return self.mediator