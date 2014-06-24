from StringIO import StringIO
from unittest import TestCase

import cmdexecutor


class CmdExecutorTest(TestCase):
    
    def setUp(self):
        self.client = cmdexecutor.app.test_client()
        cmdexecutor.app.config['UPLOAD_FOLDER'] = "/tmp"
        
    def testIndex(self):
        rv = self.client.get("/")
        self.assertEquals(rv._status_code, 200)
        self.assertEquals(rv.data, "This is REST Command Line Executor v1.0")
    
    def testRunCommand(self):
        rv = self.client.post("/cmd/dlc", data = '{"commands": ["/bin/echo", "Hello World!"]}')
        self.assertEquals(rv._status_code, 200)
        self.assertEquals(rv.data.strip(), "Hello World!")
        
    def testUpload(self):
        rv = self.client.post("/upload", data = { "file": (StringIO("my file contents\n"), "helloworld.txt")})
        self.assertEquals(rv._status_code, 200)
        