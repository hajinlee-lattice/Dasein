from leframework.model import mediator as mdtr

class StateMachine(object):
    
    def __init__(self):
        self.mediator = mdtr.Mediator()
        self.states = []
        
    def addState(self, state,jsonOrder):
        state.setMediator(self.mediator)
        state.setStateMachine(self)
        state.setJsonOrder(jsonOrder)
        self.states.append(state)
        
    def getStates(self):
        return sorted(self.states, key=lambda state: state.jsonOrder)
        
    def run(self):
        for state in self.states:
            state.execute()
    
    def getMediator(self):
        return self.mediator