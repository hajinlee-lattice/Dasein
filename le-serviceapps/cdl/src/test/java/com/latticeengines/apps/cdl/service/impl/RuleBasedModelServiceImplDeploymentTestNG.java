package com.latticeengines.apps.cdl.service.impl;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQueryConstants;
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;

public class RuleBasedModelServiceImplDeploymentTestNG extends CDLDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(RuleBasedModelServiceImplDeploymentTestNG.class);

    private static final String CREATED_BY = "lattice@lattice-engines.com";
    private static final Long RATING_A_COUNT = 4L;
    private static final Long RATING_D_COUNT = 190L;
    private static final Long RATING_F_COUNT = 9L;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private RatingEngineService ratingEngineService;

    @Inject
    private CDLTestDataService cdlTestDataService;

    private MetadataSegment reTestSegment;

    private RatingEngine rbRatingEngine;
    private String rbRatingEngineId;

    @BeforeClass(groups = "deployment")
    public void setup() throws KeyManagementException, NoSuchAlgorithmException, IOException {
        setupTestEnvironment();
        cdlTestDataService.populateData(mainTestTenant.getId());
        MetadataSegment createdSegment = segmentProxy.createOrUpdateSegment(mainTestTenant.getId(),
                constructSegment(SEGMENT_NAME));
        Assert.assertNotNull(createdSegment);
        reTestSegment = segmentProxy.getMetadataSegmentByName(mainTestTenant.getId(), createdSegment.getName());
        log.info(String.format("Created metadata segment with name %s", reTestSegment.getName()));
    }

    @Test(groups = "deployment")
    public void testCreate() {
        // Test Rulebased Rating Engine
        rbRatingEngine = createRatingEngine(RatingEngineType.RULE_BASED);
        Assert.assertEquals(rbRatingEngine.getType(), RatingEngineType.RULE_BASED);
        assertRatingEngine(rbRatingEngine);
        rbRatingEngineId = rbRatingEngine.getId();
    }

    protected RatingEngine createRatingEngine(RatingEngineType type) {
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setSegment(reTestSegment);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setType(type);
        // test basic creation
        ratingEngine = ratingEngineService.createOrUpdate(ratingEngine, mainTestTenant.getId());

        return ratingEngine;
    }

    protected void assertRatingEngine(RatingEngine createdRatingEngine) {
        Assert.assertNull(createdRatingEngine.getActiveModel());
        Assert.assertTrue(MapUtils.isEmpty(createdRatingEngine.getCountsAsMap()));
    }

    @Test(groups = "deployment", dependsOnMethods = { "testCreate" })
    public void testGetRatingEngineAndModel() {
        // test get a list
        List<RatingEngine> ratingEngineList = ratingEngineService.getAllRatingEngines();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 1);

        RatingEngine ratingEngine = ratingEngineService.getRatingEngineById(rbRatingEngineId, true, true);
        Assert.assertNotNull(ratingEngine);
        Assert.assertNotNull(ratingEngine.getActiveModelPid());

    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetRatingEngineAndModel" })
    private void testFindAndUpdateRuleBasedModel() {
        // test basic find rating models
        List<RatingModel> ratingModels = ratingEngineService.getRatingModelsByRatingEngineId(rbRatingEngineId);
        Assert.assertNotNull(ratingModels);
        Assert.assertEquals(ratingModels.size(), 1);
        Iterator<RatingModel> it = ratingModels.iterator();
        RatingModel rm = it.next();
        Assert.assertTrue(rm instanceof RuleBasedModel);
        Assert.assertEquals(rm.getIteration(), 1);
        Assert.assertEquals(((RuleBasedModel) rm).getRatingRule().getDefaultBucketName(),
                RatingRule.DEFAULT_BUCKET_NAME);

        String ratingModelId = rm.getId();
        Assert.assertNotNull(ratingModelId);
        // test get specific rating model
        rm = ratingEngineService.getRatingModel(rbRatingEngineId, ratingModelId);
        Assert.assertNotNull(rm);

        // test update rating model
        RuleBasedModel roleBasedModel = constructRuleModel();
        RatingModel retrievedRoleBasedModel = ratingEngineService.updateRatingModel(rbRatingEngineId, ratingModelId,
                roleBasedModel);
        Assert.assertTrue(retrievedRoleBasedModel instanceof RuleBasedModel);
        RatingRule ratingRule = ((RuleBasedModel) retrievedRoleBasedModel).getRatingRule();
        Assert.assertNotNull(ratingRule);
        Assert.assertEquals(ratingRule.getDefaultBucketName(), RatingBucketName.D.getName());
        Assert.assertTrue(MapUtils.isNotEmpty(ratingRule.getBucketToRuleMap()));
        Assert.assertTrue(MapUtils.isNotEmpty(ratingRule.getRuleForBucket(RatingBucketName.A)));
        Assert.assertNotNull(
                ratingRule.getRuleForBucket(RatingBucketName.A).get(FrontEndQueryConstants.ACCOUNT_RESTRICTION));
        Assert.assertTrue(MapUtils.isNotEmpty(ratingRule.getRuleForBucket(RatingBucketName.F)));
        Assert.assertNotNull(
                ratingRule.getRuleForBucket(RatingBucketName.F).get(FrontEndQueryConstants.CONTACT_RESTRICTION));

        RatingEngine ratingEngine = ratingEngineService.getRatingEngineById(rbRatingEngineId, true);
        Assert.assertTrue(MapUtils.isNotEmpty(ratingEngine.getCountsAsMap()));
        System.out.println(JsonUtils.pprint(ratingEngine));
        Assert.assertEquals(ratingEngine.getCountsAsMap().get(RatingBucketName.A.name()), RATING_A_COUNT);
        Assert.assertEquals(ratingEngine.getCountsAsMap().get(RatingBucketName.D.name()), RATING_D_COUNT);
        Assert.assertEquals(ratingEngine.getCountsAsMap().get(RatingBucketName.F.name()), RATING_F_COUNT);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testFindAndUpdateRuleBasedModel" })
    public void testDelete() {
        deleteRatingEngine(rbRatingEngineId);

        List<RatingEngine> ratingEngineList = ratingEngineService.getAllRatingEngines();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 0);
    }

    protected void deleteRatingEngine(String ratingEngineId) {
        RatingEngine ratingEngine = ratingEngineService.getRatingEngineById(ratingEngineId, false);
        String createdRatingEngineStr = ratingEngine.toString();
        log.info("Before delete, getting complete Rating Engine : " + createdRatingEngineStr);

        // test delete
        ratingEngineService.deleteById(ratingEngine.getId());
        ratingEngine = ratingEngineService.getRatingEngineById(ratingEngineId, false);
        Assert.assertNull(ratingEngine);
    }
}
