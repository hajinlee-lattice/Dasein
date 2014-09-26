package com.latticeengines.camille;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.camille.initialization.Bootstrap;
import com.latticeengines.camille.initialization.CamilleEnvironment;
import com.latticeengines.logging.DefaultAppender;
import com.latticeengines.logging.LoggerAdapter;

public class App {
	private static final Logger log = LoggerFactory.getLogger(new Object(){}.getClass().getEnclosingClass());
	
    public static void main(String[] args) throws IOException {
    	LoggerAdapter.addAppender(new DefaultAppender(System.out));
    	///////////////////////////////////////////////////////////
    	
    	ObjectMapper m = new ObjectMapper();
    	
    	Bootstrap b = null;
		try {
			b = m.readValue(new File("./src/main/resources/bootstrap.json"), Bootstrap.class);
		}
		catch (IOException e) {
			log.error(e.getMessage(), e);
			System.exit(1);
		}
    	
    	try {
			CamilleEnvironment.Initalize(b);
		}
    	catch (InterruptedException e) {
			log.error(e.getMessage(), e);
			System.exit(1);
		}
    	
    	CamilleEnvironment.stop();
    	
    	///////////////////////////////////////////////////////////
        System.exit(0);
    }
}
