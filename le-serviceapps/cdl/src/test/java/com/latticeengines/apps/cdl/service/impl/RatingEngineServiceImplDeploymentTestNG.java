package com.latticeengines.apps.cdl.service.impl;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQueryConstants;
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;

public class RatingEngineServiceImplDeploymentTestNG extends CDLDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(RatingEngineServiceImplDeploymentTestNG.class);

    private static final String RATING_ENGINE_NAME = "Rating Engine";
    private static final String RATING_ENGINE_NOTE = "This is a Rating Engine that covers North America market";
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

    private RatingEngine ratingEngine;
    private String ratingEngineId;
    private Date createdDate;
    private Date updatedDate;

    @BeforeClass(groups = "deployment")
    public void setup() throws KeyManagementException, NoSuchAlgorithmException, IOException {
        setupTestEnvironment();
        cdlTestDataService.populateData(mainTestTenant.getId());
        MetadataSegment createdSegment = segmentProxy.createOrUpdateSegment(mainTestTenant.getId(), constructSegment());
        Assert.assertNotNull(createdSegment);
        MetadataSegment retrievedSegment = segmentProxy.getMetadataSegmentByName(mainTestTenant.getId(), createdSegment.getName());
        log.info(String.format("Created metadata segment with name %s", retrievedSegment.getName()));
        ratingEngine = new RatingEngine();
        ratingEngine.setSegment(retrievedSegment);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setType(RatingEngineType.RULE_BASED);
    }

    @Test(groups = "deployment")
    public void runTests() {
        testCreate();
        testGet();
        testUpdateRatingEngine();
        testFindAndUpdateRuleBasedModel();
        testDeleteOperations();
    }

    private void testDeleteOperations() {
        RatingEngine createdRatingEngine = ratingEngineService.getRatingEngineById(ratingEngineId, false);
        String createdRatingEngineStr = createdRatingEngine.toString();
        log.info("After updating the model, the getting full of Rating Engine is " + createdRatingEngineStr);

        // test delete
        ratingEngineService.deleteById(createdRatingEngine.getId());
        List<RatingEngine> ratingEngineList = ratingEngineService.getAllRatingEngines();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 0);

        createdRatingEngine = ratingEngineService.getRatingEngineById(ratingEngineId, false);
        Assert.assertNull(createdRatingEngine);
    }

    private void testCreate() {
        // test basic creation
        RatingEngine createdRatingEngine = ratingEngineService.createOrUpdate(ratingEngine, mainTestTenant.getId());
        Assert.assertNotNull(createdRatingEngine);
        Assert.assertNotNull(createdRatingEngine.getId());
        ratingEngineId = createdRatingEngine.getId();
        Assert.assertNotNull(createdRatingEngine.getCreated());
        createdDate = createdRatingEngine.getCreated();
        Assert.assertNotNull(createdRatingEngine.getUpdated());
        updatedDate = createdRatingEngine.getUpdated();
        Assert.assertNotNull(createdRatingEngine.getDisplayName());
        Assert.assertNull(createdRatingEngine.getNote());
        Assert.assertEquals(createdRatingEngine.getType(), RatingEngineType.RULE_BASED);
        Assert.assertEquals(createdRatingEngine.getCreatedBy(), CREATED_BY);
        Assert.assertNotNull(createdRatingEngine.getRatingModels());
        Assert.assertTrue(MapUtils.isEmpty(createdRatingEngine.getCountsAsMap()));
        log.info("size of getRatingModels() " + createdRatingEngine.getRatingModels().size());
    }

    private void testGet() {
        // test get a list
        List<RatingEngine> ratingEngineList = ratingEngineService.getAllRatingEngines();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 1);
        Assert.assertEquals(ratingEngineId, ratingEngineList.get(0).getId());

        // test get a list of ratingEngine summaries
        List<RatingEngineSummary> summaries = ratingEngineService.getAllRatingEngineSummaries();
        log.info("ratingEngineSummaries is " + summaries);
        Assert.assertNotNull(summaries);
        Assert.assertEquals(summaries.size(), 1);
        Assert.assertEquals(ratingEngineId, summaries.get(0).getId());
        Assert.assertEquals(summaries.get(0).getSegmentDisplayName(), SEGMENT_NAME);
        Assert.assertEquals(summaries.get(0).getSegmentName(), ratingEngine.getSegment().getName());

        // test get list of ratingEngine summaries filtered by type and status
        summaries = ratingEngineService.getAllRatingEngineSummariesWithTypeAndStatus(null, null);
        Assert.assertEquals(summaries.size(), 1);
        summaries = ratingEngineService.getAllRatingEngineSummariesWithTypeAndStatus(RatingEngineType.AI_BASED.name(),
                null);
        Assert.assertEquals(summaries.size(), 0);
        summaries = ratingEngineService.getAllRatingEngineSummariesWithTypeAndStatus(RatingEngineType.RULE_BASED.name(),
                null);
        Assert.assertEquals(summaries.size(), 1);
        summaries = ratingEngineService.getAllRatingEngineSummariesWithTypeAndStatus(null,
                RatingEngineStatus.INACTIVE.name());
        Assert.assertEquals(summaries.size(), 1);
        summaries = ratingEngineService.getAllRatingEngineSummariesWithTypeAndStatus(null,
                RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(summaries.size(), 0);
        summaries = ratingEngineService.getAllRatingEngineSummariesWithTypeAndStatus(RatingEngineType.RULE_BASED.name(),
                RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(summaries.size(), 0);
        summaries = ratingEngineService.getAllRatingEngineSummariesWithTypeAndStatus(RatingEngineType.RULE_BASED.name(),
                RatingEngineStatus.INACTIVE.name());
        Assert.assertEquals(summaries.size(), 1);
        summaries = ratingEngineService.getAllRatingEngineSummariesWithTypeAndStatus(RatingEngineType.AI_BASED.name(),
                RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(summaries.size(), 0);
        summaries = ratingEngineService.getAllRatingEngineSummariesWithTypeAndStatus(RatingEngineType.AI_BASED.name(),
                RatingEngineStatus.INACTIVE.name());
        Assert.assertEquals(summaries.size(), 0);

        // test basic find
        RatingEngine createdRatingEngine = ratingEngineService.getRatingEngineById(ratingEngineId, false);
        Assert.assertNotNull(createdRatingEngine);
        Assert.assertEquals(ratingEngineId, createdRatingEngine.getId());
        MetadataSegment segment = createdRatingEngine.getSegment();
        Assert.assertNotNull(segment);
        Assert.assertEquals(segment.getDisplayName(), SEGMENT_NAME);
        String createdRatingEngineStr = createdRatingEngine.toString();
        createdRatingEngine = ratingEngineService.getRatingEngineById(ratingEngineId, true);
        Assert.assertNotNull(createdRatingEngine);
        log.info("String is " + createdRatingEngineStr);

        Set<RatingModel> ratingModels = createdRatingEngine.getRatingModels();
        Assert.assertNotNull(ratingModels);
        Assert.assertEquals(ratingModels.size(), 1);
        Iterator<RatingModel> it = ratingModels.iterator();
        RatingModel rm = it.next();
        Assert.assertTrue(rm instanceof RuleBasedModel);
        Assert.assertEquals(rm.getIteration(), 1);
        Assert.assertEquals(((RuleBasedModel) rm).getRatingRule().getDefaultBucketName(),
                RatingRule.DEFAULT_BUCKET_NAME);
        log.info("Rating Engine after findById is " + createdRatingEngine.toString());
    }

    private void testUpdateRatingEngine() {
        // test update rating engine
        ratingEngine.setDisplayName(RATING_ENGINE_NAME);
        ratingEngine.setNote(RATING_ENGINE_NOTE);
        ratingEngine.setStatus(RatingEngineStatus.ACTIVE);
        RatingEngine createdRatingEngine = ratingEngineService.createOrUpdate(ratingEngine, mainTestTenant.getId());
        Assert.assertEquals(RATING_ENGINE_NAME, createdRatingEngine.getDisplayName());
        Assert.assertEquals(RATING_ENGINE_NOTE, createdRatingEngine.getNote());
        Assert.assertTrue(createdRatingEngine.getUpdated().after(updatedDate));
        log.info("Created date is " + createdDate);
        log.info("The create date for the newly updated one is " + createdRatingEngine.getCreated());
        List<RatingEngine> ratingEngineList = ratingEngineService.getAllRatingEngines();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 1);
        Assert.assertEquals(ratingEngineId, ratingEngineList.get(0).getId());

        List<RatingEngineSummary> summaries = ratingEngineService.getAllRatingEngineSummariesWithTypeAndStatus(
                RatingEngineType.RULE_BASED.name(), RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(summaries.size(), 1);
        summaries = ratingEngineService.getAllRatingEngineSummariesWithTypeAndStatus(null,
                RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(summaries.size(), 1);
        summaries = ratingEngineService.getAllRatingEngineSummariesWithTypeAndStatus(RatingEngineType.RULE_BASED.name(),
                RatingEngineStatus.INACTIVE.name());
        Assert.assertEquals(summaries.size(), 0);
        summaries = ratingEngineService.getAllRatingEngineSummariesWithTypeAndStatus(null,
                RatingEngineStatus.INACTIVE.name());
        Assert.assertEquals(summaries.size(), 0);
    }

    private void testFindAndUpdateRuleBasedModel() {
        // test basic find rating models
        Set<RatingModel> ratingModels = ratingEngineService.getRatingModelsByRatingEngineId(ratingEngineId);
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
        rm = ratingEngineService.getRatingModel(ratingEngineId, ratingModelId);
        Assert.assertNotNull(rm);

        // test update rating model
        RuleBasedModel roleBasedModel = constructRuleModel();
        RatingModel retrievedRoleBasedModel = ratingEngineService.updateRatingModel(ratingEngineId, ratingModelId,
                roleBasedModel);
        Assert.assertTrue(retrievedRoleBasedModel instanceof RuleBasedModel);
        RatingRule ratingRule = ((RuleBasedModel) retrievedRoleBasedModel).getRatingRule();
        Assert.assertNotNull(ratingRule);
        Assert.assertEquals(ratingRule.getDefaultBucketName(), RuleBucketName.D.getName());
        Assert.assertTrue(MapUtils.isNotEmpty(ratingRule.getBucketToRuleMap()));
        Assert.assertTrue(MapUtils.isNotEmpty(ratingRule.getRuleForBucket(RuleBucketName.A)));
        Assert.assertNotNull(
                ratingRule.getRuleForBucket(RuleBucketName.A).get(FrontEndQueryConstants.ACCOUNT_RESTRICTION));
        Assert.assertTrue(MapUtils.isNotEmpty(ratingRule.getRuleForBucket(RuleBucketName.F)));
        Assert.assertNotNull(
                ratingRule.getRuleForBucket(RuleBucketName.F).get(FrontEndQueryConstants.CONTACT_RESTRICTION));

        RatingEngine ratingEngine = ratingEngineService.getRatingEngineById(ratingEngineId, true);
        Assert.assertTrue(MapUtils.isNotEmpty(ratingEngine.getCountsAsMap()));
        System.out.println(JsonUtils.pprint(ratingEngine));
        Assert.assertEquals(ratingEngine.getCountsAsMap().get(RuleBucketName.A.name()), RATING_A_COUNT);
        Assert.assertEquals(ratingEngine.getCountsAsMap().get(RuleBucketName.D.name()), RATING_D_COUNT);
        Assert.assertEquals(ratingEngine.getCountsAsMap().get(RuleBucketName.F.name()), RATING_F_COUNT);
    }

}
