from unittest import TestCase
import cmdexecutor

class CmdExecutorTest(TestCase):
    
    def setUp(self):
        self.app = cmdexecutor.app.test_client()
        
    def testIndex(self):
        rv = self.app.get("/")
        self.assertEquals(rv._status_code, 200)
        self.assertEquals(rv.data, "This is REST Command Line Executor v1.0")
    
    def testRunCommand(self):
        rv = self.app.post("/cmd/dlc", data = '{"commands": ["/bin/echo", "Hello World!"]}')
        self.assertEquals(rv._status_code, 200)
        self.assertEquals(rv.data.strip(), "Hello World!")
        