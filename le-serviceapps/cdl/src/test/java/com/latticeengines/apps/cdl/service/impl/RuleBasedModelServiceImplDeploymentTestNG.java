package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
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
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQueryConstants;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;

public class RuleBasedModelServiceImplDeploymentTestNG extends CDLDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(RuleBasedModelServiceImplDeploymentTestNG.class);

    private static final String CREATED_BY = "lattice@lattice-engines.com";
    private static final Long RATING_A_COUNT = 2L;
    private static final Long RATING_D_COUNT = 212L;
    private static final Long RATING_F_COUNT = 10L;

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
    public void setup() {
        setupTestEnvironment();
        cdlTestDataService.populateData(mainTestTenant.getId(), 3);

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
        cdlTestDataService.mockRatingTableWithSingleEngine(mainTestTenant.getId(), rbRatingEngine.getId(),
                generateCoverageMap());
        Assert.assertEquals(rbRatingEngine.getType(), RatingEngineType.RULE_BASED);
        assertRatingEngine(rbRatingEngine);
        rbRatingEngineId = rbRatingEngine.getId();
    }

    protected RatingEngine createRatingEngine(RatingEngineType type) {
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setSegment(reTestSegment);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setUpdatedBy(CREATED_BY);
        ratingEngine.setType(type);
        // test basic creation
        ratingEngine = ratingEngineService.createOrUpdate(ratingEngine);

        return ratingEngine;
    }

    @SuppressWarnings("deprecation")
    protected void assertRatingEngine(RatingEngine createdRatingEngine) {
        Assert.assertNotNull(createdRatingEngine.getLatestIteration());
        Assert.assertTrue(MapUtils.isEmpty(createdRatingEngine.getCountsAsMap()));
    }

    @SuppressWarnings("deprecation")
    @Test(groups = "deployment", dependsOnMethods = { "testCreate" })
    public void testGetRatingEngineAndModel() {
        // test get a list
        List<RatingEngine> ratingEngineList = ratingEngineService.getAllRatingEngines();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 1);

        RatingEngine ratingEngine = ratingEngineService.getRatingEngineById(rbRatingEngineId, true, true);
        Assert.assertNotNull(ratingEngine);
        Assert.assertNotNull(ratingEngine.getLatestIteration());
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
        RuleBasedModel ruleBasedModel = constructRuleModel();
        RatingModel retrievedRuleBasedModel = ratingEngineService.updateRatingModel(rbRatingEngineId, ratingModelId,
                ruleBasedModel);
        Assert.assertTrue(retrievedRuleBasedModel instanceof RuleBasedModel);
        RatingRule ratingRule = ((RuleBasedModel) retrievedRuleBasedModel).getRatingRule();

        Assert.assertNotNull(ratingRule);
        Assert.assertEquals(ratingRule.getDefaultBucketName(), RatingBucketName.D.getName());
        Assert.assertTrue(MapUtils.isNotEmpty(ratingRule.getBucketToRuleMap()));
        Assert.assertTrue(MapUtils.isNotEmpty(ratingRule.getRuleForBucket(RatingBucketName.A)));
        Assert.assertNotNull(
                ratingRule.getRuleForBucket(RatingBucketName.A).get(FrontEndQueryConstants.ACCOUNT_RESTRICTION));
        Assert.assertTrue(MapUtils.isNotEmpty(ratingRule.getRuleForBucket(RatingBucketName.F)));
        Assert.assertNotNull(
                ratingRule.getRuleForBucket(RatingBucketName.F).get(FrontEndQueryConstants.CONTACT_RESTRICTION));

        ratingEngineService.updateRatingEngineCounts(rbRatingEngineId);
        RatingEngine ratingEngine = ratingEngineService.getRatingEngineById(rbRatingEngineId, true);
        Assert.assertTrue(MapUtils.isNotEmpty(ratingEngine.getCountsAsMap()));
        System.out.println(JsonUtils.pprint(ratingEngine));
    }

    @Test(groups = "deployment", dependsOnMethods = { "testFindAndUpdateRuleBasedModel" })
    private void testGetDependentAttrsInAllModels() {
        List<AttributeLookup> attributes = ratingEngineService.getDependentAttrsInAllModels(rbRatingEngineId);
        Assert.assertNotNull(attributes);
        Assert.assertTrue(attributes.size() > 0);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetDependentAttrsInAllModels" })
    private void testGetDependentAttrsInActiveModel() {
        List<AttributeLookup> attributes = ratingEngineService.getDependentAttrsInActiveModel(rbRatingEngineId);
        Assert.assertNotNull(attributes);
        Assert.assertTrue(attributes.size() > 0);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetDependentAttrsInActiveModel" })
    private void testGetDependingRatingModels() {
        List<String> attributes = new ArrayList<>();
        attributes.add("Contact.ContactName");
        attributes.add("Account.Other");

        List<RatingModel> ratingModels = ratingEngineService.getDependingRatingModels(attributes);
        Assert.assertNotNull(ratingModels);
        Assert.assertEquals(ratingModels.size(), 1);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetDependingRatingModels" })
    private void testGetDependingRatingEngines() {
        List<String> attributes = new ArrayList<>();
        attributes.add("Contact.ContactName");
        attributes.add("Account.Other");

        List<RatingEngine> ratingEngines = ratingEngineService.getDependingRatingEngines(attributes);
        Assert.assertNotNull(ratingEngines);
        Assert.assertEquals(ratingEngines.size(), 1);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetDependingRatingEngines" })
    public void testDelete() {
        deleteRatingEngine(rbRatingEngineId);

        List<RatingEngine> ratingEngineList = ratingEngineService.getAllRatingEngines();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 0);
    }

    private List<BucketMetadata> generateCoverageMap() {
        return Arrays.asList(//
                new BucketMetadata(BucketName.A, RATING_A_COUNT.intValue()), //
                new BucketMetadata(BucketName.D, RATING_D_COUNT.intValue()), //
                new BucketMetadata(BucketName.F, RATING_F_COUNT.intValue()));
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
