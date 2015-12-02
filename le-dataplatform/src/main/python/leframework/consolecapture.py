from cStringIO import StringIO
import sys
import threading
from leframework.progressreporter import ProgressReporter
import re

class Capture(list):
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self
    
    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        sys.stdout = self._stdout
        self._stringio.close()
    
    def getCaptureList(self):
        return self._stringio.getvalue().splitlines()
    
    def resetCapture(self):
        oldStandout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        oldStandout.close()
            
class CaptureMonitor(threading.Thread):
    def __init__(self, capture,  previousWeight, currentWeight, totalStages, runtimeProperties, capturePattern):
        super(CaptureMonitor, self).__init__()
        self.capture = capture
        self.stages = totalStages
        self.currentStages = 0
        if runtimeProperties is not None:
            self.progressReporter = ProgressReporter(runtimeProperties["host"], int(runtimeProperties["port"]))
        else:
            self.progressReporter = ProgressReporter(None, 0)
        self.progressReporter.setTotalState(totalStages)
        self.capturePattern = capturePattern
        
        self.stop = threading.Event()
        self.previousWeight = previousWeight
        self.currentWeight = currentWeight
    
    def run(self):
        while not self.stop.isSet():
            captureList = self.capture.getCaptureList()
            self.capture.resetCapture()
            captureStages = sum(map(lambda x: 1 if re.match(self.capturePattern, x) else 0, captureList))
            if (captureStages > 0):
                self.currentStages += captureStages;
                self.progressReporter.nextStateForPreStateMachine(self.previousWeight, self.currentWeight, self.currentStages)
            else:
                self.stop.wait(5)
    
    def shutdown(self):
        self.stop.set()
    
    
            
