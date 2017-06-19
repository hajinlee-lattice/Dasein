package com.latticeengines.scoringapi.controller;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.RemoteLedpException;
import com.latticeengines.domain.exposed.scoringapi.EnrichRequest;
import com.latticeengines.domain.exposed.scoringapi.EnrichResponse;
import com.latticeengines.network.exposed.scoringapi.ScoringApiEnrichInterface;

public class EnrichResourceDeploymentTestNG extends ScoringResourceDeploymentTestNGBase {

    @Autowired
    private ScoringApiEnrichInterface scoringApiEnrichProxy;

    @Test(groups = "deployment", enabled = true)
    public void enrichRecord() throws IOException {
        EnrichRequest enrichRequest = new EnrichRequest();
        enrichRequest.setDomain("mojesbscg@salesforce.com");
        enrichRequest.setCompany("Arrow Electronics");
        enrichRequest.setState("New York");
        enrichRequest.setCountry("United States");
        enrichRequest.setDUNS("ABC");

        EnrichResponse enrichResponse = scoringApiEnrichProxy.enrichRecord(enrichRequest, customerSpace.toString(),
                "123456");

        Assert.assertNotNull(enrichResponse.getEnrichmentAttributeValues());
        System.out.println("scoreResponse.getEnrichmentAttributeValues().size() = "
                + enrichResponse.getEnrichmentAttributeValues().size() + "\n\n" + JsonUtils.serialize(enrichResponse));
        Assert.assertTrue(enrichResponse.getEnrichmentAttributeValues().size() == 6);
    }

    @Test(groups = "deployment", enabled = true)
    public void enrichRecordWithCompany() throws IOException {
        EnrichRequest enrichRequest = new EnrichRequest();
        enrichRequest.setDomain("");
        enrichRequest.setCompany("Arrow Electronics");
        enrichRequest.setState("New York");
        enrichRequest.setCountry("United States");
        enrichRequest.setDUNS("");

        EnrichResponse enrichResponse = scoringApiEnrichProxy.enrichRecord(enrichRequest, customerSpace.toString(),
                "123456");

        Assert.assertNotNull(enrichResponse.getEnrichmentAttributeValues());
        System.out.println("scoreResponse.getEnrichmentAttributeValues().size() = "
                + enrichResponse.getEnrichmentAttributeValues().size() + "\n\n" + JsonUtils.serialize(enrichResponse));
        Assert.assertTrue(enrichResponse.getEnrichmentAttributeValues().size() == 6);
    }

    @Test(groups = "deployment", enabled = true)
    public void enrichRecordWithDuns() throws IOException {
        EnrichRequest enrichRequest = new EnrichRequest();
        enrichRequest.setDomain("");
        enrichRequest.setCompany("");
        enrichRequest.setState("");
        enrichRequest.setCountry("");
        enrichRequest.setDUNS("ABC");

        EnrichResponse enrichResponse = scoringApiEnrichProxy.enrichRecord(enrichRequest, customerSpace.toString(),
                "123456");
        Assert.assertNotNull(enrichResponse.getEnrichmentAttributeValues());
        System.out.println("scoreResponse.getEnrichmentAttributeValues().size() = "
                + enrichResponse.getEnrichmentAttributeValues().size() + "\n\n" + JsonUtils.serialize(enrichResponse));
        Assert.assertTrue(enrichResponse.getEnrichmentAttributeValues().size() == 6);
    }

    @Test(groups = "deployment", enabled = true)
    public void enrichRecordFail() throws IOException {
        EnrichRequest enrichRequest = new EnrichRequest();
        enrichRequest.setDomain("");
        enrichRequest.setCompany("");
        enrichRequest.setState("New York");
        enrichRequest.setCountry("United States");
        enrichRequest.setDUNS("");

        boolean exceptionThrown = false;
        try {
            scoringApiEnrichProxy.enrichRecord(enrichRequest, customerSpace.toString(), "123456");
        } catch (RemoteLedpException e) {
            exceptionThrown = true;
            Assert.assertTrue(e.getMessage().contains(LedpCode.LEDP_31199.getExternalCode()));
        }
        Assert.assertTrue(exceptionThrown);
    }

}
