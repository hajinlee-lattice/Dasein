package com.latticeengines.camille;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.logging.DefaultAppender;
import com.latticeengines.logging.LoggerAdapter;

public class App {
	private static final Logger log = LoggerFactory.getLogger(new Object(){}.getClass().getEnclosingClass());
	
	// TODO: remove
    public static void main(String[] args) {
    	LoggerAdapter.addAppender(new DefaultAppender(System.out));
    	
        ArgumentParser parser = ArgumentParsers.newArgumentParser("prog");
        parser.addArgument("-foo");
        try {
        	log.info("foo = {}", parser.parseArgs(args).getString("foo"));
		}
        catch (ArgumentParserException e) {
			log.error(e.getMessage(), e);
		}
    	
        try {
			log.info(
				new ObjectMapper().writeValueAsString(
					new Object() {
						@SuppressWarnings("unused")
						public String getAddress() {return "53 State Street";}
						@SuppressWarnings("unused")
						public String getCompany() {return "Lattice Engines";}
			        }
				)
			);
		}
        catch (JsonProcessingException e) {
			log.error(e.getMessage(), e);
		}
        
        System.exit(0);
    }
}
