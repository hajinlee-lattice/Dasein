package com.latticeengines.marketoharness;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.scoringharness.cloudmodel.BaseCloudRead;
import com.latticeengines.scoringharness.cloudmodel.BaseCloudResult;
import com.latticeengines.scoringharness.cloudmodel.BaseCloudUpdate;
import com.latticeengines.scoringharness.marketoharness.MarketoHarness;
import com.latticeengines.scoringharness.marketoharness.MarketoProperties;

// TODO Find a way to not have to explicitly mention each class here and instead scan a package
@ContextConfiguration(classes = { MarketoHarness.class, MarketoProperties.class })
public class MarketoHarnessUnitTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private MarketoHarness marketoHarness;

    @Test(groups = "unit")
    public void testGetAccessToken() throws Exception {
        String accessToken = marketoHarness.getAccessToken();
        Assert.assertTrue(accessToken != null && !accessToken.isEmpty(), "No access token was obtained.");
    }

    @Test(groups = "unit")
    public void testInsertMarketoLeads() throws Exception {
        String accessToken = marketoHarness.getAccessToken();
        BaseCloudUpdate update = new BaseCloudUpdate(MarketoHarness.OBJECT_TYPE_LEAD,
                MarketoHarness.OBJECT_ACTION_CREATE_ONLY);
        update.addRow("{\"email\":\"testharness2@lattice-engines.com\"}");

        BaseCloudResult result = marketoHarness.updateObjects(accessToken, update);
        Assert.assertTrue(result != null, "Result was null");
        Assert.assertTrue(result.isSuccess, "success was false");
        Assert.assertTrue(result.requestId != null && !result.requestId.trim().isEmpty(), "requestId was null or empty");
        Assert.assertTrue(result.jsonObjectResults.size() == update.jsonObjects.size(),
                "Result row count did not match Update row count.");
    }

    @Test(groups = "unit")
    public void testRetrieveMarketoLeads() throws Exception {
        String accessToken = marketoHarness.getAccessToken();
        BaseCloudRead read = new BaseCloudRead(MarketoHarness.OBJECT_TYPE_LEAD, "1");
        BaseCloudResult result = marketoHarness.getObjects(accessToken, read);
        Assert.assertTrue(result != null, "Result was null");
        Assert.assertTrue(result.isSuccess, "success was false");
        Assert.assertTrue(result.requestId != null && !result.requestId.trim().isEmpty(), "requestId was null or empty");
        Assert.assertTrue(result.jsonObjectResults.size() == read.ids.size(),
                "Number of rows returned doesn't match number of rows requested");
    }

    @Test(groups = "unit")
    public void testRetrieveMarketoLeadsWithFields() throws Exception {
        String accessToken = marketoHarness.getAccessToken();
        BaseCloudRead read = new BaseCloudRead(MarketoHarness.OBJECT_TYPE_LEAD, "1");
        read.fields.add("email");
        BaseCloudResult result = marketoHarness.getObjects(accessToken, read);
        Assert.assertTrue(result != null, "Result was null");
        Assert.assertTrue(result.isSuccess, "success was false");
        Assert.assertTrue(result.requestId != null && !result.requestId.trim().isEmpty(), "requestId was null or empty");
        Assert.assertTrue(result.jsonObjectResults.size() == read.ids.size(),
                "Number of rows returned doesn't match number of rows requested");
    }
}
