package com.latticeengines.camille;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.logging.DefaultAppender;
import com.latticeengines.logging.LoggerAdapter;

public class Wrapper 
{
	private static final Logger log = LoggerFactory.getLogger(new Object(){}.getClass().getEnclosingClass());
	
    public static void main( String[] args )
    {
    	LoggerAdapter.addAppender(new DefaultAppender(System.out));
    	
    	log.info("This is {} saying hello!", new Object(){}.getClass().getEnclosingClass().getSimpleName());
    	
    	com.latticeengines.camille.App.main(new String[]{"-foo", "bar"});
    	
        System.exit(0); // required to kill the logging thread
    }
}
