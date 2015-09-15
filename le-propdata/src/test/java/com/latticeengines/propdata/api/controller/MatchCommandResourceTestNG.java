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

import com.latticeengines.domain.exposed.ResponseDocument;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-propdata-context.xml" })
public class MatchCommandResourceTestNG extends AbstractTestNGSpringContextTests{

    @Value("${propdata.api.hostport}")
    private String hostPort;
    
    protected String getRestAPIHostPort() {
        return hostPort;
    }

    private RestTemplate restTemplate = new RestTemplate();
    
    @SuppressWarnings("rawtypes")
    @Test(groups =  "disable")
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
        
        ResponseEntity<ResponseDocument> responseID = 
            restTemplate.exchange(builder.build().encode().toUri(), HttpMethod.PUT
            ,requestEntity,ResponseDocument.class);
        assertNotNull(responseID);
        
        try {
            Thread.sleep(30000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        ResponseDocument commandStatus = 
            restTemplate.getForObject(getRestAPIHostPort() 
                + "/PropData/matchcommands/" + responseID.getBody().getResult()
                ,ResponseDocument.class);
          assertNotNull(commandStatus);
    }
    
}
