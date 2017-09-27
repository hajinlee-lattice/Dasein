//package com.latticeengines.pls.service.impl;
//
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Set;
//
//import org.springframework.beans.factory.annotation.Autowired;
//import org.testng.Assert;
//import org.testng.annotations.BeforeClass;
//import org.testng.annotations.Test;
//
//import com.latticeengines.common.exposed.util.JsonUtils;
//import com.latticeengines.domain.exposed.pls.Play;
//import com.latticeengines.domain.exposed.pls.RatingEngine;
//import com.latticeengines.domain.exposed.pls.RatingModel;
//import com.latticeengines.domain.exposed.pls.RatingModelIdPair;
//import com.latticeengines.domain.exposed.pls.RatingRule;
//import com.latticeengines.domain.exposed.pls.RatingsCountRequest;
//import com.latticeengines.domain.exposed.pls.RatingsCountResponse;
//import com.latticeengines.domain.exposed.pls.RuleBasedModel;
//import com.latticeengines.domain.exposed.pls.SegmentIdAndModelRulesPair;
//import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
//import com.latticeengines.pls.service.RatingCoverageService;
//import com.latticeengines.pls.service.RatingEngineService;
//import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
//
//public class RatingCoverageServiceImplDeploymentTestNG extends PlsDeploymentTestNGBase {
//
//    private static final String DUMMY_ID = "DUMMY_ID";
//
//    @Autowired
//    private RatingCoverageService ratingCoverageService;
//
//    @Autowired
//    private RatingEngineService ratingEngineService;
//
//    @Autowired
//    private TestPlayCreationHelper testPlayCreationHelper;
//
//    private Play play;
//
//    private EntityProxy entityProxy;
//
//    private RatingEngine ratingEngine;
//
//    private RatingRule ratingRule;
//
//    @BeforeClass(groups = "deployment")
//    public void setup() throws Exception {
//        testPlayCreationHelper.setupTenantAndCreatePlay();
//
//        entityProxy = testPlayCreationHelper.initEntityProxy();
//
//        play = testPlayCreationHelper.getPlay();
//
//        ((RatingCoverageServiceImpl) ratingCoverageService).setEntityProxy(entityProxy);
//
//        ratingEngine = ratingEngineService.getRatingEngineById(play.getRatingEngine().getId(), false);
//
//        Assert.assertNotNull(ratingEngine);
//        Set<RatingModel> ratingModels = ratingEngine.getRatingModels();
//        Assert.assertNotNull(ratingModels);
//        Assert.assertTrue(ratingModels.size() > 0);
//        Assert.assertTrue(new ArrayList<>(ratingModels).get(0) instanceof RuleBasedModel);
//        RuleBasedModel ruleBasedModel = (RuleBasedModel) new ArrayList<>(ratingModels).get(0);
//
//        ratingRule = ruleBasedModel.getRatingRule();
//
//        Assert.assertNotNull(ratingRule);
//        Assert.assertNotNull(ratingRule.getBucketToRuleMap());
//        Assert.assertNotNull(ratingRule.getDefaultBucketName());
//
//    }
//
//    @Test(groups = "deployment")
//    public void testRatingIdCoverage() {
//        RatingsCountRequest request = new RatingsCountRequest();
//
//        List<String> ratingEngineIds = Arrays.asList(new String[] { ratingEngine.getId(), DUMMY_ID });
//        request.setRatingEngineIds(ratingEngineIds);
//        RatingsCountResponse response = ratingCoverageService.getCoverageInfo(request, false);
//        Assert.assertNotNull(response);
//        Assert.assertNotNull(response.getRatingEngineIdCoverageMap());
//        Assert.assertNull(response.getRatingEngineModelIdCoverageMap());
//        Assert.assertNull(response.getSegmentIdCoverageMap());
//        Assert.assertNull(response.getSegmentIdModelRulesCoverageMap());
//
//        Assert.assertEquals(response.getRatingEngineIdCoverageMap().size(), 1);
//
//        for (String ratingId : ratingEngineIds) {
//            if (ratingId.equals(DUMMY_ID)) {
//                Assert.assertNotNull(response.getErrorMap());
//                Assert.assertTrue(response.getErrorMap().containsKey(ratingId));
//                Assert.assertNotNull(response.getErrorMap().get(ratingId));
//                Assert.assertFalse(response.getRatingEngineIdCoverageMap().containsKey(ratingId));
//            } else {
//                Assert.assertTrue(response.getRatingEngineIdCoverageMap().containsKey(ratingId));
//                Assert.assertNotNull(response.getRatingEngineIdCoverageMap().get(ratingId));
//                Assert.assertFalse(response.getErrorMap().containsKey(ratingId));
//
//                Set<String> uniqueBuckets = new HashSet<>();
//                uniqueBuckets.addAll(ratingRule.getBucketToRuleMap().keySet());
//                uniqueBuckets.add(ratingRule.getDefaultBucketName());
//
//                System.out.println(uniqueBuckets);
//                System.out.println(JsonUtils.serialize(response.getRatingEngineIdCoverageMap().get(ratingId)));
//            }
//        }
//    }
//
//    @Test(groups = "deployment")
//    public void testSegmentIdCoverage() {
//        RatingsCountRequest request = new RatingsCountRequest();
//        List<String> segmentIds = Arrays
//                .asList(new String[] { play.getRatingEngine().getSegment().getName(), DUMMY_ID });
//        request.setSegmentIds(segmentIds);
//        RatingsCountResponse response = ratingCoverageService.getCoverageInfo(request, false);
//        Assert.assertNotNull(response);
//        Assert.assertNull(response.getRatingEngineIdCoverageMap());
//        Assert.assertNull(response.getRatingEngineModelIdCoverageMap());
//        Assert.assertNotNull(response.getSegmentIdCoverageMap());
//        Assert.assertNull(response.getSegmentIdModelRulesCoverageMap());
//
//        Assert.assertEquals(response.getSegmentIdCoverageMap().size(), 1);
//
//        for (String segmentId : segmentIds) {
//            if (segmentId.equals(DUMMY_ID)) {
//                Assert.assertNotNull(response.getErrorMap());
//                Assert.assertTrue(response.getErrorMap().containsKey(segmentId));
//                Assert.assertNotNull(response.getErrorMap().get(segmentId));
//                Assert.assertFalse(response.getSegmentIdCoverageMap().containsKey(segmentId));
//            } else {
//                Assert.assertTrue(response.getSegmentIdCoverageMap().containsKey(segmentId));
//                Assert.assertNotNull(response.getSegmentIdCoverageMap().get(segmentId));
//                Assert.assertFalse(response.getErrorMap().containsKey(segmentId));
//            }
//        }
//    }
//
//    @Test(groups = "deployment")
//    public void testRatingModelIdCoverage() {
//        RatingsCountRequest request = new RatingsCountRequest();
//        RatingModelIdPair p1 = new RatingModelIdPair();
//        p1.setRatingEngineId("e1");
//        p1.setRatingModelId("m1");
//        RatingModelIdPair p2 = new RatingModelIdPair();
//        p2.setRatingEngineId("e2");
//        p2.setRatingModelId("m2");
//        List<RatingModelIdPair> ratingEngineModelIds = Arrays.asList(new RatingModelIdPair[] { p1, p2 });
//        request.setRatingEngineModelIds(ratingEngineModelIds);
//        RatingsCountResponse response = ratingCoverageService.getCoverageInfo(request, true);
//        Assert.assertNotNull(response);
//        Assert.assertNull(response.getRatingEngineIdCoverageMap());
//        Assert.assertNotNull(response.getRatingEngineModelIdCoverageMap());
//        Assert.assertNull(response.getSegmentIdCoverageMap());
//        Assert.assertNull(response.getSegmentIdModelRulesCoverageMap());
//
//        Assert.assertEquals(response.getRatingEngineModelIdCoverageMap().size(), ratingEngineModelIds.size());
//
//        for (RatingModelIdPair ratingModelId : ratingEngineModelIds) {
//            Assert.assertTrue(
//                    response.getRatingEngineModelIdCoverageMap().containsKey(ratingModelId.getRatingModelId()));
//            Assert.assertNotNull(response.getRatingEngineModelIdCoverageMap().get(ratingModelId.getRatingModelId()));
//        }
//    }
//
//    @Test(groups = "deployment")
//    public void testSegmentIdModelRulesCoverage() {
//        RatingsCountRequest request = new RatingsCountRequest();
//        SegmentIdAndModelRulesPair r1 = new SegmentIdAndModelRulesPair();
//        r1.setSegmentId("sg1");
//        RatingRule ratingRule = new RatingRule();
//        r1.setRatingRule(ratingRule);
//        List<SegmentIdAndModelRulesPair> segmentIdModelRules = Arrays.asList(new SegmentIdAndModelRulesPair[] { r1 });
//        request.setSegmentIdModelRules(segmentIdModelRules);
//        RatingsCountResponse response = ratingCoverageService.getCoverageInfo(request, true);
//        Assert.assertNotNull(response);
//        Assert.assertNull(response.getRatingEngineIdCoverageMap());
//        Assert.assertNull(response.getRatingEngineModelIdCoverageMap());
//        Assert.assertNull(response.getSegmentIdCoverageMap());
//        Assert.assertNotNull(response.getSegmentIdModelRulesCoverageMap());
//
//        Assert.assertEquals(response.getSegmentIdModelRulesCoverageMap().size(), segmentIdModelRules.size());
//
//        for (SegmentIdAndModelRulesPair segmentIdModelPair : segmentIdModelRules) {
//            Assert.assertTrue(
//                    response.getSegmentIdModelRulesCoverageMap().containsKey(segmentIdModelPair.getSegmentId()));
//            Assert.assertNotNull(response.getSegmentIdModelRulesCoverageMap().get(segmentIdModelPair.getSegmentId()));
//        }
//    }
//
//    @Test(groups = "deployment")
//    public void testSegmentIdSingleRulesCoverage() {
//        RatingsCountRequest request = new RatingsCountRequest();
//        SegmentIdAndModelRulesPair r1 = new SegmentIdAndModelRulesPair();
//        r1.setSegmentId("sg1");
//        RatingRule ratingRule = new RatingRule();
//        r1.setRatingRule(ratingRule);
//        List<SegmentIdAndModelRulesPair> segmentIdModelRules = Arrays.asList(new SegmentIdAndModelRulesPair[] { r1 });
//        request.setSegmentIdModelRules(segmentIdModelRules);
//        RatingsCountResponse response = ratingCoverageService.getCoverageInfo(request, true);
//        Assert.assertNotNull(response);
//        Assert.assertNull(response.getRatingEngineIdCoverageMap());
//        Assert.assertNull(response.getRatingEngineModelIdCoverageMap());
//        Assert.assertNull(response.getSegmentIdCoverageMap());
//        Assert.assertNotNull(response.getSegmentIdModelRulesCoverageMap());
//
//        Assert.assertEquals(response.getSegmentIdModelRulesCoverageMap().size(), segmentIdModelRules.size());
//
//        for (SegmentIdAndModelRulesPair segmentIdModelPair : segmentIdModelRules) {
//            Assert.assertTrue(
//                    response.getSegmentIdModelRulesCoverageMap().containsKey(segmentIdModelPair.getSegmentId()));
//            Assert.assertNotNull(response.getSegmentIdModelRulesCoverageMap().get(segmentIdModelPair.getSegmentId()));
//        }
//    }
//}
