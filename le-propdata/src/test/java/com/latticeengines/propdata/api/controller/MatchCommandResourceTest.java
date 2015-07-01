package com.latticeengines.propdata.api.controller;

import static org.testng.Assert.assertNotNull;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.ResponseCommandStatus;
import com.latticeengines.domain.exposed.propdata.ResponseID;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-propdata-context.xml" })
public class MatchCommandResourceTest extends AbstractTestNGSpringContextTests{

	@Value("${propdata.api.hostport}")
    private String hostPort;
	
	protected String getRestAPIHostPort() {
        return hostPort;
    }

	private RestTemplate restTemplate = new RestTemplate();
	
	@Test
	public void testMatchCommands() {
		Object sourceTable = new String("PayPal_matching_elements_small");
		Object destTables = new String("Alexa_Source|DerivedColumns");
		Object contractExternalID = new String("PD_Test");
		Object matchClient = new String("10.51.15.130");
		HttpHeaders headers = new HttpHeaders();
	    headers.add("Content-Type", "application/json");
	    headers.add("Accept", "application/json");
	    HttpEntity<String> requestEntity = new HttpEntity<>("", headers);
	    UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(getRestAPIHostPort() + "/PropData/matchcommands/")
	            .queryParam("sourceTable", sourceTable)
	            .queryParam("destTables", destTables)
	            .queryParam("contractExternalID", contractExternalID)
	            .queryParam("matchClient", matchClient);
	    
		ResponseEntity<ResponseID> responseID = restTemplate.exchange(builder.build().encode().toUri(), HttpMethod.PUT
				,requestEntity,ResponseID.class);
	    assertNotNull(responseID);
	    
	    try {
			Thread.sleep(30000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	    
	    ResponseCommandStatus commandStatus = 
	    		restTemplate.getForObject(getRestAPIHostPort() + "/PropData/matchcommands/" + responseID.getBody().getID(),ResponseCommandStatus.class);
	      assertNotNull(commandStatus);
	}
	
}
