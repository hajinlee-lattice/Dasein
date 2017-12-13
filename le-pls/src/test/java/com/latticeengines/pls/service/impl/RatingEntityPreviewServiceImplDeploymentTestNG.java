package com.latticeengines.pls.service.impl;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

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

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RatingsCountRequest;
import com.latticeengines.domain.exposed.pls.RatingsCountResponse;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.pls.service.RatingCoverageService;
import com.latticeengines.pls.service.RatingEntityPreviewService;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-pls-context.xml" })
public class RatingEntityPreviewServiceImplDeploymentTestNG extends AbstractTestNGSpringContextTests {

    private static final String RATING_BUCKET_FIELD = "RATING_BUCKET_FIELD";

    @Autowired
    private RatingEntityPreviewService ratingEntityPreviewService;

    @Autowired
    private RatingCoverageService ratingCoverageService;

    @Autowired
    private RatingEngineProxy ratingEngineProxy;

    @Autowired
    private TestPlayCreationHelper testPlayCreationHelper;

    private Play play;

    private EntityProxy entityProxy;

    private RatingEngine ratingEngine;

    private RatingRule ratingRule;

    private Long segmentAccountsCount = null;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        testPlayCreationHelper.setupTenantAndCreatePlay();

        entityProxy = testPlayCreationHelper.initEntityProxy();

        play = testPlayCreationHelper.getPlay();

        ((RatingEntityPreviewServiceImpl) ratingEntityPreviewService).setEntityProxy(entityProxy);

        ratingEngine = ratingEngineProxy.getRatingEngine(testPlayCreationHelper.getTenant().getId(),
                play.getRatingEngine().getId());

        Assert.assertNotNull(ratingEngine);
        Assert.assertNotNull(ratingEngine.getSegment());
        Assert.assertNotNull(ratingEngine.getSegment().getAccountRestriction());
        Assert.assertNotNull(ratingEngine.getSegment().getContactRestriction());

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
    public void testEntityPreviewFirstTime() {
        testEntityPreview(0L, 5L);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testEntityPreviewFirstTime" })
    public void testEntityPreviewSecondTime() {
        // it is imp to run same query twice to be able to test caching effect
        testEntityPreview(0L, 5L);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testEntityPreviewSecondTime" })
    public void testEntityPreviewThirdTimeWithDifferentPages() {
        Set<String> accIds1 = testEntityPreview(0L, 5L);
        Set<String> accIds2 = testEntityPreview(5L, 5L);

        Assert.assertTrue(accIds1.size() > 0);
        Assert.assertTrue(accIds2.size() > 0);

        accIds1.stream().forEach(id -> Assert.assertFalse(accIds2.contains(id)));
        accIds2.stream().forEach(id -> Assert.assertFalse(accIds1.contains(id)));
    }

    @Test(groups = "deployment", dependsOnMethods = { "testEntityPreviewThirdTimeWithDifferentPages" })
    public void testEntityPreviewThirdTimeWithPartiallyOverlappingPages() {
        Set<String> accIds1 = testEntityPreview(0L, 5L);
        Set<String> accIds2 = testEntityPreview(2L, 5L);

        Assert.assertTrue(accIds1.size() > 0);
        Assert.assertTrue(accIds2.size() > 0);

        AtomicInteger overlappingCount = new AtomicInteger();
        accIds1.stream().forEach(id -> {
            if (accIds2.contains(id)) {
                overlappingCount.incrementAndGet();
            }
        });
        Assert.assertEquals(overlappingCount.get(), 3);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testEntityPreviewThirdTimeWithPartiallyOverlappingPages" })
    public void testGetSegmentAccountCount() {
        RatingsCountRequest request = new RatingsCountRequest();
        List<String> segmentIds = Arrays.asList(play.getRatingEngine().getSegment().getName());
        request.setSegmentIds(segmentIds);
        RatingsCountResponse response = ratingCoverageService.getCoverageInfo(request);
        Assert.assertNotNull(response);
        Assert.assertNull(response.getRatingEngineIdCoverageMap());
        Assert.assertNull(response.getRatingEngineModelIdCoverageMap());
        Assert.assertNotNull(response.getSegmentIdCoverageMap());
        Assert.assertNull(response.getSegmentIdModelRulesCoverageMap());

        Assert.assertEquals(response.getSegmentIdCoverageMap().size(), 1);

        for (String segmentId : segmentIds) {
            Assert.assertTrue(response.getSegmentIdCoverageMap().containsKey(segmentId));
            Assert.assertNotNull(response.getSegmentIdCoverageMap().get(segmentId));
            Assert.assertFalse(
                    response.getErrorMap().get(RatingCoverageService.SEGMENT_IDS_ERROR_MAP_KEY).containsKey(segmentId));
            segmentAccountsCount = response.getSegmentIdCoverageMap().get(segmentId).getAccountCount();
        }
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetSegmentAccountCount" })
    public void testEntityPreviewFourthTimeToTestEdgeCase() {
        Set<String> accIds1 = testEntityPreview(segmentAccountsCount - 3L, 6L);
        Assert.assertEquals(accIds1.size(), 3);
        Set<String> accIds2 = testEntityPreview(segmentAccountsCount - 2L, 6L);
        Assert.assertEquals(accIds2.size(), 2);
        Set<String> accIds3 = testEntityPreview(segmentAccountsCount, 6L, false);
        Assert.assertEquals(accIds3.size(), 0);
        Set<String> accIds4 = testEntityPreview(segmentAccountsCount + 10, 6L, false);
        Assert.assertEquals(accIds4.size(), 0);
    }

    public Set<String> testEntityPreview(Long offset, Long max) {
        return testEntityPreview(offset, max, true);
    }

    public Set<String> testEntityPreview(Long offset, Long max, boolean expectingSomeAccounts) {
        DataPage response = ratingEntityPreviewService.getEntityPreview(ratingEngine, offset, max,
                BusinessEntity.Account, InterfaceName.AccountId.name(), false, RATING_BUCKET_FIELD, null, false, null,
                null);
        Assert.assertNotNull(response);
        Assert.assertNotNull(response.getData());

        if (expectingSomeAccounts) {
            Assert.assertTrue(response.getData().size() > 0);
        }

        Set<String> accIds = new HashSet<>();
        response.getData() //
                .stream() //
                .forEach(row -> {
                    Assert.assertNotNull(row.get(RATING_BUCKET_FIELD));
                    Assert.assertNotNull(row.get(InterfaceName.AccountId.name()));
                    Assert.assertFalse(accIds.contains(row.get(InterfaceName.AccountId.name()).toString()));
                    accIds.add(row.get(InterfaceName.AccountId.name()).toString());
                });
        return accIds;
    }
}
