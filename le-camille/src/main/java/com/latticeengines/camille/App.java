package com.latticeengines.camille;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.CamilleEnvironment.Mode;
import com.latticeengines.logging.DefaultAppender;
import com.latticeengines.logging.LoggerAdapter;

public class App {
	private static final Logger log = LoggerFactory.getLogger(new Object(){}.getClass().getEnclosingClass());
	
    public static void main(String[] args) {
    	LoggerAdapter.addAppender(new DefaultAppender(System.out));
    	
    	
		try {
			CamilleEnvironment.start(Mode.RUNTIME);
		}
		catch (IllegalStateException | InterruptedException | IOException e) {
			log.error("Error starting Camille environment", e);
			System.exit(1);
		}
    	
    	
    	Camille camille = CamilleEnvironment.getCamille();
    	
    	
    	log.info(camille.toString());
    	
    	
    	CamilleEnvironment.stop();
    	
    	
        System.exit(0);
    }
}
