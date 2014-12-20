package com.latticeengines.marketoharness;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.latticeengines.cloudmodel.BaseCloudResult;
import com.latticeengines.cloudmodel.BaseCloudUpdate;

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
	
	public void testInsertMarketoLeads() throws Exception {
		String accessToken = MarketoUtilities.getAccessToken();
		BaseCloudUpdate update = new BaseCloudUpdate(
				MarketoUtilities.OBJECT_TYPE_LEAD,
				MarketoUtilities.OBJECT_ACTION_CREATE_ONLY);
		update.addRow("{\"email\":\"testharness2@lattice-engines.com\"}");
		
		BaseCloudResult result = MarketoUtilities.updateObjects(accessToken, update);
		assertTrue("Result was null", result != null);
		assertTrue("success was false", result.isSuccess);
		assertTrue("requestId was null or empty", result.requestId != null && !result.requestId.trim().isEmpty());
		assertTrue("Result row count did not match Update row count.", 
				result.jsonObjectResults.size() == result.update.jsonObjects.size());
	}
}
