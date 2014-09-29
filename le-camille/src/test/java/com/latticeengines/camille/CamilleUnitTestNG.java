package com.latticeengines.camille;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.StringReader;

import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.camille.CamilleEnvironment.Mode;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.netflix.curator.test.TestingServer;

public class CamilleUnitTestNG {

	private static final Logger log = LoggerFactory.getLogger(new Object(){}.getClass().getEnclosingClass());
	
	private static TestingServer initTestServerAndCamille() throws Exception {
		try {
	    	TestingServer server = new TestingServer();
	    	
	    	CamilleEnvironment.stop();
	    	
	    	ConfigJson config = new ConfigJson();
	    	config.setConnectionString(server.getConnectString());
	    	config.setPodId("testPodId");
	    	
	    	OutputStream oStream = new ByteArrayOutputStream();
	    	
	    	new ObjectMapper().writeValue(oStream, config);
	    	
			CamilleEnvironment.start(Mode.RUNTIME, new StringReader(oStream.toString()));
			
			return server;
		}
		catch (Exception e) {
			log.error("Error starting Camille environment", e);
			throw e;
		}
	}
	
    @Test(groups = "unit")
    public void testCreate() throws Exception {
    	TestingServer server = null;
        try {
        	server = initTestServerAndCamille();
	        
	        Camille c = CamilleEnvironment.getCamille();
	        
	        Path path = new Path("/testPath");
	        Document doc =  new Document("testData", null);
	        
	        c.create(path, doc, ZooDefs.Ids.READ_ACL_UNSAFE);
	        
	        Assert.assertTrue(c.exists(path));
	        Assert.assertFalse(c.exists(new Path("/testWrongPath")));
	        Assert.assertEquals(c.get(path).getData(), doc.getData());
        }
        finally {
        	CamilleEnvironment.stop();
        	if (server != null) server.close();
        }
    }
}
