package com.latticeengines.camille;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
	private static final Logger log = LoggerFactory.getLogger(new Object(){}.getClass().getEnclosingClass());
	
    public static void main(String[] args) {	
    	
    	log.info("Hello, {}!", "Lattice");
    	
        System.exit(0);
    }
}
