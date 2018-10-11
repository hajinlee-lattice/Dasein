import logging
import re
import socket


class ProgressReporter(object):
    '''
    This class tracks the python modeling progress in terms of states and reports to PythonAppMaster
    '''

    def __init__(self, amHost, amPort, maxTry = 1, stateCount = 0, startState = 0):
        self.logger = logging.getLogger(name = 'progressReporter')
        self.host = self.parseHost(amHost)
        self.port = amPort
        self.maxTry = maxTry

        self.progress = 0
        self.preStateMachineWeight = float(1) / 3
        self.postStateMachineWeight = float(1) / 3
        self.stateMachineWeight = 1 - self.preStateMachineWeight - self.postStateMachineWeight

        self.curState = startState
        self.stateCount = stateCount

    # this is a temp workaround for emr
    # better approach to set it correctly in AM
    def parseHost(self, host):
        if host is not None and len(host) > 3 and host[:3] == "ip-":
            pattern = re.compile("ip-\\d+-\\d+-\\d+-\\d+")
            match = pattern.match(host)
            if match:
                ip = match.group(0)[3:]
                ip = ip.replace("-", ".")
                self.logger.error("Change AM host from %s to %s" % (host, ip))
                host = ip
        return host

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

    def nextStateForPreStateMachine(self, previousWeight, currentWeight, curState):
        self.curState = curState
        self.progress = previousWeight + currentWeight * self.curState / float(self.stateCount)
        self.__reportProgress()

    def __reportProgress(self):
        if self.host is None or self.port == 0:
            self.logger.warn("Couldn't connect to a null host")
            return

        self.logger.info("Reporting progress: " + str(self.progress))
        for _ in range(self.maxTry):
            try:
                skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                skt.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                skt.connect((self.host, self.port))
                skt.send(str(self.progress) + '\n')
                skt.close()
                break
            except socket.error:
                self.logger.error("Failed to send progress to: " + self.host + " at port: " + str(self.port))





