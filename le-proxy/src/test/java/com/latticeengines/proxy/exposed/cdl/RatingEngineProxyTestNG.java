package com.latticeengines.proxy.exposed.cdl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

@ContextConfiguration(locations = { "classpath:test-proxy-context.xml" })
public class RatingEngineProxyTestNG extends AbstractTestNGSpringContextTests {

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    private static final String customerSpace = "tenant";
    private static final String user = "user";
    private static final String ratingEngineId = "ratingEngineId";
    private static final String modelId = "modelId";
    private static final Logger log = LoggerFactory.getLogger(RatingEngineProxyTestNG.class);

    @Test(groups = "functional")
    public void testConstructCreateOrUpdateRatingEngineUrl() {
        String url = ratingEngineProxy.constructCreateOrUpdateRatingEngineUrl(customerSpace, null, null, null);
        log.info("url:" + url);
        Assert.assertTrue(url.contains("/customerspaces/tenant/ratingengines"));

        url = ratingEngineProxy.constructCreateOrUpdateRatingEngineUrl(customerSpace, user, null, null);
        log.info("url:" + url);
        Assert.assertTrue(url.contains("/customerspaces/tenant/ratingengines?user=user"));

        url = ratingEngineProxy.constructCreateOrUpdateRatingEngineUrl(customerSpace, null, false, null);
        log.info("url:" + url);
        Assert.assertTrue(url.contains("/customerspaces/tenant/ratingengines?unlink-segment=false"));

        url = ratingEngineProxy.constructCreateOrUpdateRatingEngineUrl(customerSpace, null, null, false);
        log.info("url:" + url);
        Assert.assertTrue(url.contains("/customerspaces/tenant/ratingengines?create-action=false"));

        url = ratingEngineProxy.constructCreateOrUpdateRatingEngineUrl(customerSpace, null, true, false);
        log.info("url:" + url);
        Assert.assertTrue(url.contains("/customerspaces/tenant/ratingengines?unlink-segment=true&create-action=false"));

        url = ratingEngineProxy.constructCreateOrUpdateRatingEngineUrl(customerSpace, user, false, false);
        log.info("url:" + url);
        Assert.assertTrue(url
                .contains("/customerspaces/tenant/ratingengines?user=user&unlink-segment=false&create-action=false"));
    }

    @Test(groups = "functional")
    public void testConstructUpdateRatingModelUrl() {
        String url = ratingEngineProxy.constructUpdateRatingModelUrl(customerSpace, ratingEngineId, modelId, null);
        log.info("url:" + url);
        Assert.assertTrue(url.contains("/customerspaces/tenant/ratingengines/ratingEngineId/ratingmodels/modelId"));
        url = ratingEngineProxy.constructUpdateRatingModelUrl(customerSpace, ratingEngineId, modelId, user);
        log.info("url:" + url);
        Assert.assertTrue(
                url.contains("/customerspaces/tenant/ratingengines/ratingEngineId/ratingmodels/modelId?user=user"));

    }
}
