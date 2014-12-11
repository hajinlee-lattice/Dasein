package com.latticeengines.marketoharness;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class MarketoUtilitiesTest extends TestCase {

	public MarketoUtilitiesTest(String name) {
		super(name);
	}

    public static Test suite() {
        return new TestSuite( MarketoUtilitiesTest.class );
    }
    
	public void testGetAccessToken() throws Exception {
		String accessToken = MarketoUtilities.getAccessToken();
		assertTrue("No access token was obtained.",
				accessToken != null && !accessToken.isEmpty());
	}
}
