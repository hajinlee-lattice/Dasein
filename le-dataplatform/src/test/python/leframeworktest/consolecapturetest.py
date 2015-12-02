from testbase import TestBase
from leframework.consolecapture import Capture, CaptureMonitor
from time import sleep

class ConsoleCaptureTest(TestBase):

    def testCapture(self):
        with Capture() as capture:
            print "List1"
            print "List2"
            self.assertEqual(capture.getCaptureList(), ["List1", "List2"])
            capture.resetCapture()
            self.assertEqual(capture.getCaptureList(), [])
            print "List3"
            self.assertEqual(capture.getCaptureList(), ["List3"])
            
    def testCaptureMonitor(self):
        with Capture() as capture:
            print "building tree1"
            print "building tree2"
            print "building tree3"
            print "building forest1"
            print "creating forest2"
            monitor = CaptureMonitor(capture, 0.1, 0.223, 5, None, "building tree.*")
            monitor.start()
            sleep(1)
            self.assertEqual(monitor.currentStages, 3)
            
            monitor.shutdown()
            
            
            
        

