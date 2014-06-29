import logging
import socket

class ProgressReporter(object):
    '''
    This class tracks the python modeling progress in terms of states and reports to PythonAppMaster
    '''
    
    def __init__(self, amHost, amPort, stateCount = 0, startState = 0):
        self.logger = logging.getLogger(name = 'progressReporter')
        self.host = amHost
        self.port = amPort
        self.maxRetry = 5
        
        self.progress = 0
        self.preStateMachineWeight = float(1) / 3
        self.postStateMachineWeight = float(1) / 3
        self.stateMachineWeight = 1 - self.preStateMachineWeight - self.postStateMachineWeight 
         
        self.curState = startState
        self.stateCount = stateCount
        
    def setTotalState(self, size):
        if size < 0:
            self.logger.error("Number of states cannot be negative")
            return   
        self.stateCount = size
    
    def setStartState(self, state):
        if state < 0 or state > self.stateCount:
            self.logger.error("Number of states cannot be negative")
            return 
        self.curState = state
    
    def inStateMachine(self):
        self.progress = self.preStateMachineWeight
        self.__reportProgress()
        
    def nextState(self):
        self.curState += 1
        self.progress = self.preStateMachineWeight + self.stateMachineWeight * self.curState / float(self.stateCount)
        self.__reportProgress()
        
    def __reportProgress(self):
        if self.host is None or self.port == 0:
            self.logger.warn("Couldn't connect to a null host")
            return
        
        self.logger.info("Reporting progress: " + str(self.progress))
        for _ in range(self.maxRetry): 
            try:
                skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                skt.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                skt.connect((self.host, self.port))
                skt.send(str(self.progress) + '\n')
                skt.close()
                break
            except socket.error:
                self.logger.error("Failed to send progress to: " + self.host + "at port: " + str(self.port))
            
            
        
        
            
