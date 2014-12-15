package com.latticeengines.marketoharness;

import org.apache.http.client.fluent.Request;

public class MarketoObjectUtilities {

	
	public MarketoObjectUtilities() {
		// TODO Auto-generated constructor stub
	}

	public static void createLead() {
		String leadEndpointUrl = MarketoUtilities.getLeadEndpointUrl();
		Request.Post(leadEndpointUrl);
	}
}
