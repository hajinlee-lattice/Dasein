from abc import ABCMeta, abstractmethod

class State(object):
    __metaclass__ = ABCMeta
    
    def __init__(self, name):
        self.name = name
    
    @abstractmethod
    def execute(self): pass
    
    def getName(self):
        return self.name
    
    def setMediator(self, mediator):
        self.mediator = mediator
    
    def getMediator(self):
        return self.mediator

    def setStateMachine(self, stateMachine):
        self.stateMachine = stateMachine
    
    def getStateMachine(self):
        return self.stateMachine
    