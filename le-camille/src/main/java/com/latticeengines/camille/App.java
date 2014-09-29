package com.latticeengines.camille;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.camille.CamilleEnvironment.Mode;
import com.netflix.curator.test.TestingServer;

public class App {
	private static final Logger log = LoggerFactory.getLogger(new Object(){}.getClass().getEnclosingClass());
	
    public static void main(String[] args) {	
    	
    	TestingServer server = null;
    	try {
			server = new TestingServer();
		}
    	catch (Exception e) {
			log.error("Error creating Curator test server.", e);
			System.exit(1);
		}
    	
    	ConfigJson config = new ConfigJson();
    	config.setConnectionString(server.getConnectString());
    	config.setPodId("testPodId");
    	
    	ObjectMapper mapper = new ObjectMapper();
    	
    	ByteArrayOutputStream oStream = new ByteArrayOutputStream();
    	
    	try {
			mapper.writeValue(oStream, config);
		}
    	catch (IOException e) {
			log.error("Error writing to byte array.", e);
			System.exit(1);
		}
    	
		try {
			CamilleEnvironment.start(Mode.RUNTIME, new StringReader(oStream.toString()));
		}
		catch (IllegalStateException | InterruptedException | IOException e) {
			log.error("Error starting Camille environment", e);
			System.exit(1);
		}
    	
    	log.info("Curator test server state = {}", CamilleEnvironment.getCamille().getCuratorClient().getState());
    	
    	CamilleEnvironment.stop();
    	
    	try {
			server.stop();
		}
    	catch (IOException e) {
			log.error("Error stopping Curator test server", e);
			System.exit(1);
		}
    	
        System.exit(0);
    }
}
