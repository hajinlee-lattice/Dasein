package com.latticeengines.camille;

import java.io.File;
import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class CamilleEnvironment {
	public enum Mode {
		BOOTSTRAP,
		RUNTIME
	};
	
	private static final Logger log = LoggerFactory.getLogger(new Object(){}.getClass().getEnclosingClass());
	
    // these are reasonable arguments for the ExponentialBackoffRetry. The first
    // retry will wait 1 second - the second will wait up to 2 seconds - the
    // third will wait up to 4 seconds.
    private static final ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
	
	// singleton instance
	private static Camille camille = null;
	
	// TODO: accept inputstream with camille.json
	public static void start(Mode mode) throws IllegalStateException, IOException, InterruptedException {
		if (camille != null &&
			camille.getCuratorClient() != null &&
			camille.getCuratorClient().getState().equals(CuratorFrameworkState.STARTED)) {
			
			IllegalStateException ise = new IllegalStateException("Camille environment is already started");
			log.error(ise.getMessage(), ise);
			camille = null;
			throw ise;
		}
		
		NullPointerException npe = new NullPointerException("mode cannot be null");
		if (mode == null) {
			log.error(npe.getMessage(), npe);
			camille = null;
			throw npe;
		}
		
    	ConfigJson config = null;
		try {
			config = new ObjectMapper().readValue(new File("camille.json"), ConfigJson.class);
		}
		catch (IOException ioe) {
			log.error("An error occurred reading camille.json.", ioe);
			camille = null;
			throw ioe;
		}
		
		CuratorFramework client = CuratorFrameworkFactory.newClient(config.getConnectionString(), retryPolicy);
		client.start();
		try {
			client.blockUntilConnected();
		}
		catch (InterruptedException ie) {
			log.error("Waiting for Curator connection was interrupted.", ie);
			camille = null;
			throw ie;
		}
		
		// TODO: do something useful in this case statement
		switch (mode) {
		case BOOTSTRAP: // ignore Pod ID
			
			break;
		case RUNTIME: // use Pod ID
			
			break;
		}
		
		camille = new Camille(client);
	}
	
	public static void stop() {
		if (camille != null &&
				camille.getCuratorClient() != null &&
				camille.getCuratorClient().getState().equals(CuratorFrameworkState.STARTED)) {
			camille.getCuratorClient().close();
			camille = null;
		}
	}
	
	public static Camille getCamille() {
		return camille;
	}
}
