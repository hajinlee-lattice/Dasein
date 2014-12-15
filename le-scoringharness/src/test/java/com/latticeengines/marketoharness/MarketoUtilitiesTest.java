package com.latticeengines.marketoharness;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.util.*;

import org.apache.http.client.fluent.Response;

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
		ArrayList<HashMap<String,String>> leads = new ArrayList<HashMap<String,String>>();
		HashMap<String,String> lead = new HashMap<String,String>();
		leads.add(lead);
		lead.put("email", "testharness2@lattice-engines.com");
		
		Response response = MarketoUtilities.insertMarketoLeads(accessToken, leads);
		assertTrue(response != null);
	}
}
