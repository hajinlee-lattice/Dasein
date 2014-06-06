from abc import ABCMeta, abstractmethod
import logging

class State(object):
    __metaclass__ = ABCMeta
    
    def __init__(self, name):
        self.name = name
        logging.basicConfig(level=logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    
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
    
    def setJsonOrder(self,jsonOrder):
        self.jsonOrder = jsonOrder
    
    def getJsonOrder(self):
        return self.jsonOrder
    