package com.latticeengines.scoringapi.controller;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.RemoteLedpException;
import com.latticeengines.domain.exposed.scoringapi.EnrichRequest;
import com.latticeengines.domain.exposed.scoringapi.EnrichResponse;
import com.latticeengines.proxy.exposed.scoringapi.ScoringApiEnrichProxy;

public class EnrichResourceDeploymentTestNG extends ScoringResourceDeploymentTestNGBase {

    @Inject
    private ScoringApiEnrichProxy scoringApiEnrichProxy;

    @Test(groups = "deployment")
    public void enrichRecord() {
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
        Assert.assertEquals(enrichResponse.getEnrichmentAttributeValues().size(), 6);
    }

    @Test(groups = "deployment")
    public void enrichRecordWithCompany() {
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
        Assert.assertEquals(enrichResponse.getEnrichmentAttributeValues().size(), 6);
    }

    @Test(groups = "deployment")
    public void enrichRecordWithDuns() {
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
        Assert.assertEquals(enrichResponse.getEnrichmentAttributeValues().size(), 6);
    }

    @Test(groups = "deployment")
    public void enrichRecordFail() {
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
