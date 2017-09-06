package com.latticeengines.pls.service.impl;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.RatingModelIdPair;
import com.latticeengines.domain.exposed.pls.RatingsCountRequest;
import com.latticeengines.domain.exposed.pls.RatingsCountResponse;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.service.RatingCoverageService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class RatingCoverageServiceImplDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Autowired
    private RatingCoverageService ratingCoverageService;

    @BeforeClass(groups = "deployment")
    public void setup() throws KeyManagementException, NoSuchAlgorithmException, IOException {
        setupTestEnvironmentWithOneTenant();
        MultiTenantContext.setTenant(mainTestTenant);
        mainTestTenant = testBed.getMainTestTenant();
        switchToSuperAdmin();
    }

    @Test(groups = "deployment")
    public void testRatingIdCoverage() {
        RatingsCountRequest request = new RatingsCountRequest();
        List<String> ratingEngineIds = Arrays.asList(new String[] { "a1", "a2", "a3" });
        request.setRatingEngineIds(ratingEngineIds);
        RatingsCountResponse response = ratingCoverageService.getCoverageInfo(request);
        Assert.assertNotNull(response);
        Assert.assertNotNull(response.getRatingEngineIdCoverageMap());
        Assert.assertNull(response.getRatingEngineModelIdCoverageMap());
        Assert.assertNull(response.getSegmentIdCoverageMap());

        Assert.assertEquals(response.getRatingEngineIdCoverageMap().size(), ratingEngineIds.size());

        for (String ratingId : ratingEngineIds) {
            Assert.assertTrue(response.getRatingEngineIdCoverageMap().containsKey(ratingId));
            Assert.assertNotNull(response.getRatingEngineIdCoverageMap().get(ratingId));
        }
    }

    @Test(groups = "deployment")
    public void testSegmentIdCoverage() {
        RatingsCountRequest request = new RatingsCountRequest();
        List<String> segmentIds = Arrays.asList(new String[] { "s1", "s2", "s3" });
        request.setSegmentIds(segmentIds);
        RatingsCountResponse response = ratingCoverageService.getCoverageInfo(request);
        Assert.assertNotNull(response);
        Assert.assertNull(response.getRatingEngineIdCoverageMap());
        Assert.assertNull(response.getRatingEngineModelIdCoverageMap());
        Assert.assertNotNull(response.getSegmentIdCoverageMap());

        Assert.assertEquals(response.getSegmentIdCoverageMap().size(), segmentIds.size());

        for (String ratingId : segmentIds) {
            Assert.assertTrue(response.getSegmentIdCoverageMap().containsKey(ratingId));
            Assert.assertNotNull(response.getSegmentIdCoverageMap().get(ratingId));
        }
    }

    @Test(groups = "deployment")
    public void testRatingModelIdCoverage() {
        RatingsCountRequest request = new RatingsCountRequest();
        RatingModelIdPair p1 = new RatingModelIdPair();
        p1.setRatingEngineId("e1");
        p1.setRatingModelId("m1");
        RatingModelIdPair p2 = new RatingModelIdPair();
        p2.setRatingEngineId("e2");
        p2.setRatingModelId("m2");
        List<RatingModelIdPair> ratingEngineModelIds = Arrays.asList(new RatingModelIdPair[] { p1, p2 });
        request.setRatingEngineModelIds(ratingEngineModelIds);
        RatingsCountResponse response = ratingCoverageService.getCoverageInfo(request);
        Assert.assertNotNull(response);
        Assert.assertNull(response.getRatingEngineIdCoverageMap());
        Assert.assertNotNull(response.getRatingEngineModelIdCoverageMap());
        Assert.assertNull(response.getSegmentIdCoverageMap());

        Assert.assertEquals(response.getRatingEngineModelIdCoverageMap().size(), ratingEngineModelIds.size());

        for (RatingModelIdPair ratingModelId : ratingEngineModelIds) {
            Assert.assertTrue(response.getRatingEngineModelIdCoverageMap().containsKey(ratingModelId));
            Assert.assertNotNull(response.getRatingEngineModelIdCoverageMap().get(ratingModelId));
        }
    }
}
