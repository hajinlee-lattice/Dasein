package com.latticeengines.apps.cdl.service.impl;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.RatingCoverageService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.ratings.coverage.CoverageInfo;
import com.latticeengines.domain.exposed.ratings.coverage.RatingIdLookupColumnPair;
import com.latticeengines.domain.exposed.ratings.coverage.RatingModelIdPair;
import com.latticeengines.domain.exposed.ratings.coverage.RatingsCountRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingsCountResponse;
import com.latticeengines.domain.exposed.ratings.coverage.SegmentIdAndModelRulesPair;
import com.latticeengines.domain.exposed.ratings.coverage.SegmentIdAndSingleRulePair;
import com.latticeengines.proxy.exposed.cdl.RatingCoverageProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-serviceapps-cdl-context.xml" })
public class RatingCoverageServiceImplDeploymentTestNG extends AbstractTestNGSpringContextTests {

    private static final String DUMMY_ID = "DUMMY_ID";

    @Autowired
    private RatingCoverageProxy ratingCoverageProxy;

    @Autowired
    private RatingEngineProxy ratingEngineProxy;

    @Autowired
    private TestPlayCreationHelper testPlayCreationHelper;

    private Play play;

    private RatingEngine ratingEngine;

    private RatingRule ratingRule;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        testPlayCreationHelper.setupTenantAndCreatePlay();

        play = testPlayCreationHelper.getPlay();
        ratingEngine = ratingEngineProxy.getRatingEngine(testPlayCreationHelper.getTenant().getId(),
                play.getRatingEngine().getId());

        Assert.assertNotNull(ratingEngine);
        List<RatingModel> ratingModels = ratingEngineProxy.getRatingModels(testPlayCreationHelper.getTenant().getId(),
                play.getRatingEngine().getId());
        Assert.assertNotNull(ratingModels);
        Assert.assertTrue(ratingModels.size() > 0);
        Assert.assertTrue(ratingModels.get(0) instanceof RuleBasedModel);
        RuleBasedModel ruleBasedModel = (RuleBasedModel) ratingEngine.getActiveModel();
        Assert.assertNotNull(ruleBasedModel);

        ratingRule = ruleBasedModel.getRatingRule();

        Assert.assertNotNull(ratingRule);
        Assert.assertNotNull(ratingRule.getBucketToRuleMap());
        Assert.assertNotNull(ratingRule.getDefaultBucketName());

    }

    @AfterClass(groups = { "deployment" })
    public void teardown() throws Exception {
        testPlayCreationHelper.cleanupArtifacts();
    }

    @Test(groups = "deployment")
    public void testRatingIdCoverage() {
        RatingsCountRequest request = new RatingsCountRequest();

        List<String> ratingEngineIds = Arrays.asList(ratingEngine.getId(), DUMMY_ID);
        request.setRatingEngineIds(ratingEngineIds);
        RatingsCountResponse response = ratingCoverageProxy.getCoverageInfo(testPlayCreationHelper.getTenant().getId(),
                request);
        Assert.assertNotNull(response);
        Assert.assertNotNull(response.getRatingEngineIdCoverageMap());
        Assert.assertNull(response.getRatingEngineModelIdCoverageMap());
        Assert.assertNull(response.getSegmentIdCoverageMap());
        Assert.assertNull(response.getSegmentIdModelRulesCoverageMap());
        Assert.assertNull(response.getRatingIdLookupColumnPairsCoverageMap());

        Assert.assertEquals(response.getRatingEngineIdCoverageMap().size(), 1);

        for (String ratingId : ratingEngineIds) {
            if (ratingId.equals(DUMMY_ID)) {
                Assert.assertNotNull(response.getErrorMap());
                Assert.assertTrue(response.getErrorMap().get(RatingCoverageService.RATING_IDS_ERROR_MAP_KEY)
                        .containsKey(ratingId));
                Assert.assertNotNull(
                        response.getErrorMap().get(RatingCoverageService.RATING_IDS_ERROR_MAP_KEY).get(ratingId));
                Assert.assertFalse(response.getRatingEngineIdCoverageMap().containsKey(ratingId));
            } else {
                Assert.assertTrue(response.getRatingEngineIdCoverageMap().containsKey(ratingId));
                Assert.assertNotNull(response.getRatingEngineIdCoverageMap().get(ratingId));
                Assert.assertFalse(response.getErrorMap().get(RatingCoverageService.RATING_IDS_ERROR_MAP_KEY)
                        .containsKey(ratingId));

                Set<String> uniqueBuckets = new HashSet<>();
                uniqueBuckets.addAll(ratingRule.getBucketToRuleMap().keySet());
                uniqueBuckets.add(ratingRule.getDefaultBucketName());

                System.out.println(uniqueBuckets);
                System.out.println(JsonUtils.serialize(response.getRatingEngineIdCoverageMap().get(ratingId)));
            }
        }
    }

    @Test(groups = "deployment")
    public void testSegmentIdCoverage() {
        RatingsCountRequest request = new RatingsCountRequest();
        List<String> segmentIds = Arrays
                .asList(new String[] { play.getRatingEngine().getSegment().getName(), DUMMY_ID });
        request.setSegmentIds(segmentIds);
        RatingsCountResponse response = ratingCoverageProxy.getCoverageInfo(testPlayCreationHelper.getTenant().getId(),
                request);
        Assert.assertNotNull(response);
        Assert.assertNull(response.getRatingEngineIdCoverageMap());
        Assert.assertNull(response.getRatingEngineModelIdCoverageMap());
        Assert.assertNotNull(response.getSegmentIdCoverageMap());
        Assert.assertNull(response.getSegmentIdModelRulesCoverageMap());
        Assert.assertNull(response.getRatingIdLookupColumnPairsCoverageMap());

        Assert.assertEquals(response.getSegmentIdCoverageMap().size(), 1);

        for (String segmentId : segmentIds) {
            if (segmentId.equals(DUMMY_ID)) {
                Assert.assertNotNull(response.getErrorMap());
                Assert.assertTrue(response.getErrorMap().get(RatingCoverageService.SEGMENT_IDS_ERROR_MAP_KEY)
                        .containsKey(segmentId));
                Assert.assertNotNull(
                        response.getErrorMap().get(RatingCoverageService.SEGMENT_IDS_ERROR_MAP_KEY).get(segmentId));
                Assert.assertFalse(response.getSegmentIdCoverageMap().containsKey(segmentId));
            } else {
                Assert.assertTrue(response.getSegmentIdCoverageMap().containsKey(segmentId));
                Assert.assertNotNull(response.getSegmentIdCoverageMap().get(segmentId));
                Assert.assertFalse(response.getErrorMap().get(RatingCoverageService.SEGMENT_IDS_ERROR_MAP_KEY)
                        .containsKey(segmentId));
            }
        }
    }

    @Test(groups = "deployment")
    public void testRatingModelIdCoverage() {
        RuleBasedModel ruleBasedModel = (RuleBasedModel) ratingEngine.getActiveModel();
        RatingsCountRequest request = new RatingsCountRequest();
        RatingModelIdPair p1 = new RatingModelIdPair();
        p1.setRatingEngineId(ratingEngine.getId());
        p1.setRatingModelId(ruleBasedModel.getId());
        RatingModelIdPair p2 = new RatingModelIdPair();
        p2.setRatingEngineId(DUMMY_ID);
        p2.setRatingModelId(DUMMY_ID);
        List<RatingModelIdPair> ratingEngineModelIds = Arrays.asList(new RatingModelIdPair[] { p1, p2 });
        request.setRatingEngineModelIds(ratingEngineModelIds);
        RatingsCountResponse response = ratingCoverageProxy.getCoverageInfo(testPlayCreationHelper.getTenant().getId(),
                request);
        Assert.assertNotNull(response);
        Assert.assertNull(response.getRatingEngineIdCoverageMap());
        Assert.assertNotNull(response.getRatingEngineModelIdCoverageMap());
        Assert.assertNull(response.getSegmentIdCoverageMap());
        Assert.assertNull(response.getSegmentIdModelRulesCoverageMap());
        Assert.assertNull(response.getRatingIdLookupColumnPairsCoverageMap());

        Assert.assertEquals(response.getRatingEngineModelIdCoverageMap().size(), ratingEngineModelIds.size());

        for (RatingModelIdPair ratingModelId : ratingEngineModelIds) {
            Assert.assertTrue(
                    response.getRatingEngineModelIdCoverageMap().containsKey(ratingModelId.getRatingModelId()));
            Assert.assertNotNull(response.getRatingEngineModelIdCoverageMap().get(ratingModelId.getRatingModelId()));
        }
    }

    @Test(groups = "deployment")
    public void testRatingIdLookupColumnPairCoverage() {
        RatingsCountRequest request = new RatingsCountRequest();
        String p1Key = "p1Key";
        String p2Key = "p2Key";
        String p3Key = "p3Key";
        String p4Key = "p4Key";
        String p5Key = null;

        RatingIdLookupColumnPair p1 = new RatingIdLookupColumnPair();
        p1.setRatingEngineId(ratingEngine.getId());
        p1.setResponseKeyId(p1Key);
        p1.setLookupColumn(InterfaceName.SalesforceAccountID.name());
        RatingIdLookupColumnPair p2 = new RatingIdLookupColumnPair();
        p2.setRatingEngineId(ratingEngine.getId());
        p2.setResponseKeyId(p2Key);
        p2.setLookupColumn(InterfaceName.CompanyName.name());
        RatingIdLookupColumnPair p3 = new RatingIdLookupColumnPair();
        p3.setRatingEngineId(ratingEngine.getId());
        p3.setResponseKeyId(p3Key);
        p3.setLookupColumn("DUMMY_COLUMN");
        RatingIdLookupColumnPair p4 = new RatingIdLookupColumnPair();
        p4.setRatingEngineId("DUMMY_ID");
        p4.setResponseKeyId(p4Key);
        p4.setLookupColumn(InterfaceName.SalesforceAccountID.name());
        RatingIdLookupColumnPair p5 = new RatingIdLookupColumnPair();
        p5.setRatingEngineId(ratingEngine.getId());
        p5.setResponseKeyId(p5Key);
        p5.setLookupColumn(InterfaceName.SalesforceAccountID.name());

        List<RatingIdLookupColumnPair> ratingIdLookupColumnPairs = Arrays
                .asList(new RatingIdLookupColumnPair[] { p1, p2, p3, p4, p5 });
        Set<RatingIdLookupColumnPair> badOnes = new HashSet<>(
                Arrays.asList(new RatingIdLookupColumnPair[] { p3, p4, p5 }));
        request.setRatingIdLookupColumnPairs(ratingIdLookupColumnPairs);
        RatingsCountResponse response = ratingCoverageProxy.getCoverageInfo(testPlayCreationHelper.getTenant().getId(),
                request);
        Assert.assertNotNull(response);
        Assert.assertNull(response.getRatingEngineIdCoverageMap());
        Assert.assertNull(response.getRatingEngineModelIdCoverageMap());
        Assert.assertNull(response.getSegmentIdCoverageMap());
        Assert.assertNull(response.getSegmentIdModelRulesCoverageMap());
        Assert.assertNotNull(response.getRatingIdLookupColumnPairsCoverageMap());

        Assert.assertEquals(response.getRatingIdLookupColumnPairsCoverageMap().size(),
                ratingIdLookupColumnPairs.size() - badOnes.size(), //
                String.format("Request = %s\nResponse = %s", //
                        JsonUtils.serialize(request), JsonUtils.serialize(response)));

        for (RatingIdLookupColumnPair ratingIdLookupColumnPair : ratingIdLookupColumnPairs) {
            String key = ratingIdLookupColumnPair.getResponseKeyId();

            if (badOnes.contains(ratingIdLookupColumnPair)) {
                Assert.assertFalse(response.getRatingIdLookupColumnPairsCoverageMap()
                        .containsKey(ratingIdLookupColumnPair.getResponseKeyId()));
                key = key == null ? "NULL_KEY" : key;
                
                Assert.assertNotNull(response.getErrorMap());
                Map<String, String> errorMap = response.getErrorMap().get("processRatingIdLookupColumnPairs");
                Assert.assertNotNull(errorMap);
                Assert.assertTrue(errorMap.containsKey(key));
                Assert.assertNotNull(errorMap.get(key));
            } else {
                Assert.assertTrue(response.getRatingIdLookupColumnPairsCoverageMap().containsKey(key));
                CoverageInfo coverage = response.getRatingIdLookupColumnPairsCoverageMap().get(key);
                Assert.assertNotNull(coverage);
                Assert.assertNotNull(coverage.getAccountCount());
                Assert.assertNotNull(coverage.getContactCount());
                Assert.assertNotNull(coverage.getBucketCoverageCounts());
            }
        }
    }

    @Test(groups = "deployment")
    public void testSegmentIdModelRulesCoverage() {
        RuleBasedModel ruleBasedModel = (RuleBasedModel) ratingEngine.getActiveModel();
        RatingsCountRequest request = new RatingsCountRequest();
        SegmentIdAndModelRulesPair r1 = new SegmentIdAndModelRulesPair();
        r1.setSegmentId(ratingEngine.getSegment().getName());
        r1.setRatingRule(ruleBasedModel.getRatingRule());
        List<SegmentIdAndModelRulesPair> segmentIdModelRules = Arrays.asList(new SegmentIdAndModelRulesPair[] { r1 });
        request.setSegmentIdModelRules(segmentIdModelRules);
        RatingsCountResponse response = ratingCoverageProxy.getCoverageInfo(testPlayCreationHelper.getTenant().getId(),
                request);
        Assert.assertNotNull(response);
        Assert.assertNull(response.getRatingEngineIdCoverageMap());
        Assert.assertNull(response.getRatingEngineModelIdCoverageMap());
        Assert.assertNull(response.getSegmentIdCoverageMap());
        Assert.assertNotNull(response.getSegmentIdModelRulesCoverageMap());
        Assert.assertNull(response.getRatingIdLookupColumnPairsCoverageMap());

        Assert.assertEquals(response.getSegmentIdModelRulesCoverageMap().size(), segmentIdModelRules.size());

        for (SegmentIdAndModelRulesPair segmentIdModelPair : segmentIdModelRules) {
            Assert.assertTrue(
                    response.getSegmentIdModelRulesCoverageMap().containsKey(segmentIdModelPair.getSegmentId()));
            Assert.assertNotNull(response.getSegmentIdModelRulesCoverageMap().get(segmentIdModelPair.getSegmentId()));
        }
    }

    @Test(groups = "deployment")
    public void testSegmentIdSingleRulesCoverage() {
        RatingsCountRequest request = new RatingsCountRequest();

        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.Account, "LDC_Name");
        Bucket bucket1 = Bucket.rangeBkt("A", "G", true, false);
        Bucket bucket2 = Bucket.rangeBkt("B", "Z", true, false);
        Restriction accountRestriction1 = new BucketRestriction(attrLookup, bucket1);
        Restriction accountRestriction2 = new BucketRestriction(attrLookup, bucket2);

        SegmentIdAndSingleRulePair r1 = new SegmentIdAndSingleRulePair();
        r1.setSegmentId(ratingEngine.getSegment().getName());
        r1.setResponseKeyId("R1");
        r1.setAccountRestriction(accountRestriction1);

        SegmentIdAndSingleRulePair r2 = new SegmentIdAndSingleRulePair();
        r2.setSegmentId(ratingEngine.getSegment().getName());
        r2.setResponseKeyId("R2");

        r2.setAccountRestriction(accountRestriction2);

        List<SegmentIdAndSingleRulePair> segmentIdAndSingleRulePairs = Arrays
                .asList(new SegmentIdAndSingleRulePair[] { r1, r2 });
        request.setSegmentIdAndSingleRules(segmentIdAndSingleRulePairs);

        RatingsCountResponse response = ratingCoverageProxy.getCoverageInfo(testPlayCreationHelper.getTenant().getId(),
                request);

        Assert.assertNotNull(response);
        Assert.assertNull(response.getRatingEngineIdCoverageMap());
        Assert.assertNull(response.getRatingEngineModelIdCoverageMap());
        Assert.assertNull(response.getSegmentIdCoverageMap());
        Assert.assertNotNull(response.getSegmentIdAndSingleRulesCoverageMap());
        Assert.assertNull(response.getRatingIdLookupColumnPairsCoverageMap());

        Assert.assertEquals(response.getSegmentIdAndSingleRulesCoverageMap().size(),
                segmentIdAndSingleRulePairs.size());

        for (SegmentIdAndSingleRulePair segmentIdAndSingleRulePair : segmentIdAndSingleRulePairs) {
            Assert.assertTrue(response.getSegmentIdAndSingleRulesCoverageMap()
                    .containsKey(segmentIdAndSingleRulePair.getResponseKeyId()));
            Assert.assertNotNull(response.getSegmentIdAndSingleRulesCoverageMap()
                    .get(segmentIdAndSingleRulePair.getResponseKeyId()));
        }
    }
}
