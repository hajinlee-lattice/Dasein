package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
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

import com.latticeengines.apps.cdl.service.RatingEntityPreviewService;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.ratings.coverage.RatingsCountRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingsCountResponse;
import com.latticeengines.proxy.exposed.cdl.RatingCoverageProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.testframework.exposed.domain.TestPlaySetupConfig;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.TestPlayCreationHelper;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-serviceapps-cdl-context.xml" })
public class RatingEntityPreviewServiceImplDeploymentTestNG extends AbstractTestNGSpringContextTests {

    private static final String RATING_BUCKET_FIELD = "RATING_BUCKET_FIELD";

    @Autowired
    private RatingEntityPreviewService ratingEntityPreviewService;

    @Autowired
    private RatingCoverageProxy ratingCoverageProxy;

    @Autowired
    private RatingEngineProxy ratingEngineProxy;

    @Autowired
    private TestPlayCreationHelper testPlayCreationHelper;

    private Play play;

    private RatingEngine ratingEngine;

    private Long segmentAccountsCount = null;

    private Set<String> actualRatingBucketsInSegment = new HashSet<>();

    private String actualNameInOneOfTheAccounts = null;

    @BeforeClass(groups = "deployment-app")
    public void setup() throws Exception {
        Map<String, Boolean> featureFlags = new HashMap<>();
        featureFlags.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName(), false);
        featureFlags.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA.getName(), false);
        TestPlaySetupConfig plConfig = new TestPlaySetupConfig.Builder().featureFlags(featureFlags).build();

        testPlayCreationHelper.setupTenantAndCreatePlay(plConfig);

        EntityProxy entityProxy = testPlayCreationHelper.initEntityProxy();

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
        RuleBasedModel ruleBasedModel = (RuleBasedModel) ratingEngine.getLatestIteration();
        Assert.assertNotNull(ruleBasedModel);

        RatingRule ratingRule = ruleBasedModel.getRatingRule();

        Assert.assertNotNull(ratingRule);
        Assert.assertNotNull(ratingRule.getBucketToRuleMap());
        Assert.assertNotNull(ratingRule.getDefaultBucketName());

    }

    @AfterClass(groups = { "deployment-app" })
    public void teardown() throws Exception {
        testPlayCreationHelper.cleanupArtifacts(true);
    }

    @Test(groups = "deployment-app")
    public void testEntityPreviewFirstTime() {
        testEntityPreview(0L, 5L);
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testEntityPreviewFirstTime" })
    public void testEntityPreviewSecondTime() {
        // it is imp to run same query twice to be able to test caching effect
        testEntityPreview(0L, 5L);
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testEntityPreviewSecondTime" })
    public void testEntityPreviewThirdTimeWithDifferentPages() {
        Set<String> accIds0 = testEntityPreview(0L, 5L);
        if (accIds0.size() < 2) {
            return;
        }
        Set<String> accIds1 = testEntityPreview(0L, 1L);
        Set<String> accIds2 = testEntityPreview(1L, 1L);

        Assert.assertTrue(accIds1.size() > 0);
        Assert.assertTrue(accIds2.size() > 0);

        accIds1.forEach(id -> Assert.assertFalse(accIds2.contains(id)));
        accIds2.forEach(id -> Assert.assertFalse(accIds1.contains(id)));
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testEntityPreviewThirdTimeWithDifferentPages" })
    public void testEntityPreviewThirdTimeWithPartiallyOverlappingPages() {
        Set<String> accIds0 = testEntityPreview(0L, 5L);
        if (accIds0.size() < 2) {
            return;
        }
        Set<String> accIds1 = testEntityPreview(0L, 2L);
        Set<String> accIds2 = testEntityPreview(1L, 2L);

        Assert.assertTrue(accIds1.size() > 0);
        Assert.assertTrue(accIds2.size() > 0);

        AtomicInteger overlappingCount = new AtomicInteger();
        accIds1.forEach(id -> {
            if (accIds2.contains(id)) {
                overlappingCount.incrementAndGet();
            }
        });
        Assert.assertEquals(overlappingCount.get(), 1);
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testEntityPreviewThirdTimeWithPartiallyOverlappingPages" })
    public void testGetSegmentAccountCount() {
        RatingsCountRequest request = new RatingsCountRequest();
        List<String> segmentIds = Collections.singletonList(play.getRatingEngine().getSegment().getName());
        request.setSegmentIds(segmentIds);
        RatingsCountResponse response = ratingCoverageProxy.getCoverageInfo(testPlayCreationHelper.getTenant().getId(),
                request);
        Assert.assertNotNull(response);
        Assert.assertNull(response.getRatingEngineIdCoverageMap());
        Assert.assertNull(response.getRatingEngineModelIdCoverageMap());
        Assert.assertNotNull(response.getSegmentIdCoverageMap());
        Assert.assertNull(response.getSegmentIdModelRulesCoverageMap());

        Assert.assertEquals(response.getSegmentIdCoverageMap().size(), 1);

        for (String segmentId : segmentIds) {
            Assert.assertTrue(response.getSegmentIdCoverageMap().containsKey(segmentId));
            Assert.assertNotNull(response.getSegmentIdCoverageMap().get(segmentId));
            Assert.assertNull(response.getErrorMap());
            segmentAccountsCount = response.getSegmentIdCoverageMap().get(segmentId).getAccountCount();
        }
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testGetSegmentAccountCount" })
    public void testEntityPreviewCount() {
        List<String> allRatingBuckets = Arrays.asList(RatingBucketName.values()).stream().map(RatingBucketName::getName)
                .collect(Collectors.toList());
        List<String> partialRatingBuckets = new ArrayList<>();
        boolean removedOneBucket = false;
        for (String ratingBucket : allRatingBuckets) {
            if (!removedOneBucket && actualRatingBucketsInSegment.contains(ratingBucket)) {
                // deliberating removing one valid bucket so that we can use
                // remaining list of buckets in our count query
                removedOneBucket = true;
                continue;
            }

            partialRatingBuckets.add(ratingBucket);
        }

        long count1 = ratingEntityPreviewService.getEntityPreviewCount(ratingEngine, BusinessEntity.Account, null, null,
                null, InterfaceName.SalesforceAccountID.name());
        long count2 = ratingEntityPreviewService.getEntityPreviewCount(ratingEngine, BusinessEntity.Account, null, null,
                allRatingBuckets, InterfaceName.SalesforceAccountID.name());
        long count3 = ratingEntityPreviewService.getEntityPreviewCount(ratingEngine, BusinessEntity.Account, null, null,
                partialRatingBuckets, InterfaceName.SalesforceAccountID.name());
        long count4 = ratingEntityPreviewService.getEntityPreviewCount(ratingEngine, BusinessEntity.Account, null,
                actualNameInOneOfTheAccounts, partialRatingBuckets, InterfaceName.SalesforceAccountID.name());

        long count5 = ratingEntityPreviewService.getEntityPreviewCount(ratingEngine, BusinessEntity.Account, false,
                null, null, InterfaceName.SalesforceAccountID.name());
        long count6 = ratingEntityPreviewService.getEntityPreviewCount(ratingEngine, BusinessEntity.Account, false,
                null, allRatingBuckets, InterfaceName.SalesforceAccountID.name());
        long count7 = ratingEntityPreviewService.getEntityPreviewCount(ratingEngine, BusinessEntity.Account, false,
                null, partialRatingBuckets, InterfaceName.SalesforceAccountID.name());
        long count8 = ratingEntityPreviewService.getEntityPreviewCount(ratingEngine, BusinessEntity.Account, false,
                actualNameInOneOfTheAccounts, partialRatingBuckets, InterfaceName.SalesforceAccountID.name());

        long count9 = ratingEntityPreviewService.getEntityPreviewCount(ratingEngine, BusinessEntity.Account, true, null,
                null, InterfaceName.SalesforceAccountID.name());
        long count10 = ratingEntityPreviewService.getEntityPreviewCount(ratingEngine, BusinessEntity.Account, true,
                null, allRatingBuckets, InterfaceName.SalesforceAccountID.name());
        long count11 = ratingEntityPreviewService.getEntityPreviewCount(ratingEngine, BusinessEntity.Account, true,
                null, partialRatingBuckets, InterfaceName.SalesforceAccountID.name());
        long count12 = ratingEntityPreviewService.getEntityPreviewCount(ratingEngine, BusinessEntity.Account, true,
                actualNameInOneOfTheAccounts, partialRatingBuckets, InterfaceName.SalesforceAccountID.name());

        Assert.assertTrue(count1 == count2);
        Assert.assertTrue(count2 >= count3);
        Assert.assertTrue(count3 >= count4);

        Assert.assertTrue(count5 == count6);
        Assert.assertTrue(count6 >= count7);
        Assert.assertTrue(count7 >= count8);

        Assert.assertTrue(count9 == count10);
        Assert.assertTrue(count10 >= count11);
        Assert.assertTrue(count11 >= count12);

        Assert.assertTrue(count1 >= count5);
        Assert.assertTrue(count1 >= count9);

        Long contactCount = ratingEntityPreviewService.getEntityPreviewCount(ratingEngine, BusinessEntity.Contact, null,
                null, null, InterfaceName.SalesforceAccountID.name());
        Assert.assertNotNull(contactCount);
        Assert.assertTrue(contactCount >= 0L);
    }

    // disabled it for now as mock rating has only 2 accounts with non-null
    // rating
    @Test(groups = "deployment-app", enabled = false, dependsOnMethods = { "testEntityPreviewCount" })
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
        testEntityPreview(offset, max, expectingSomeAccounts, true);
        return testEntityPreview(offset, max, expectingSomeAccounts, false);
    }

    public Set<String> testEntityPreview(Long offset, Long max, boolean expectingSomeAccounts, boolean sortOnRating) {
        DataPage response = ratingEntityPreviewService.getEntityPreview( //
                ratingEngine, offset, max, BusinessEntity.Account, //
                sortOnRating ? RATING_BUCKET_FIELD : InterfaceName.AccountId.name(), //
                false, RATING_BUCKET_FIELD, null, false, null, null, //
                InterfaceName.SalesforceAccountID.name());
        Assert.assertNotNull(response);
        Assert.assertNotNull(response.getData());

        if (expectingSomeAccounts) {
            Assert.assertTrue(response.getData().size() > 0);
        }

        Set<String> accIds = new HashSet<>();
        response.getData() //
                .stream() //
                .forEach(row -> {
                    String rating = (String) row.get(RATING_BUCKET_FIELD);
                    if (rating != null) {
                        actualRatingBucketsInSegment.add(rating);
                    }
                    Assert.assertNotNull(row.get(InterfaceName.AccountId.name()));
                    Assert.assertFalse(accIds.contains(row.get(InterfaceName.AccountId.name()).toString()));
                    accIds.add(row.get(InterfaceName.AccountId.name()).toString());
                    String accountName = (String) row.get(InterfaceName.CompanyName.name());
                    if (actualNameInOneOfTheAccounts == null && StringUtils.isNotBlank(accountName)) {
                        actualNameInOneOfTheAccounts = accountName.trim();
                    }
                });

        if (actualRatingBucketsInSegment.size() > 0) {
            actualRatingBucketsInSegment.stream()
                    .forEach(bucket -> Assert.assertNotNull(RatingBucketName.valueOf(bucket), bucket));
        }
        return accIds;
    }
}
