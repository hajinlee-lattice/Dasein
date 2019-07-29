package com.latticeengines.objectapi.service.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQueryConstants;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndSort;
import com.latticeengines.objectapi.service.RatingQueryService;

public class RatingQueryServiceImplTestNG extends QueryServiceImplTestNGBase {

    private static final String ATTR_ACCOUNT_NAME = "CompanyName";
    private static final String ATTR_CONTACT_TITLE = "Occupation";
    private static final String VALUE_CONTACT_TITLE = "Analyst";

    @Inject
    private RatingQueryService ratingQueryService;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestData(3);
    }

    @Test(groups = "functional")
    public void testScoreData() {
        RatingModel model = ruleBasedModel();

        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction restriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("D").build();
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        Bucket contactBkt = Bucket.valueBkt(ComparisonType.CONTAINS, Collections.singletonList(VALUE_CONTACT_TITLE));
        Restriction contactRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Contact, ATTR_CONTACT_TITLE), contactBkt);
        frontEndQuery.setContactRestriction(new FrontEndRestriction(contactRestriction));
        frontEndQuery.setRatingModels(Collections.singletonList(model));
        frontEndQuery.setPageFilter(new PageFilter(0, 10));
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setSort(new FrontEndSort(
                Collections.singletonList(new AttributeLookup(BusinessEntity.Account, ATTR_ACCOUNT_NAME)), false));

        DataPage dataPage = ratingQueryService.getData(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertNotNull(dataPage);
        List<Map<String, Object>> data = dataPage.getData();
        Assert.assertFalse(data.isEmpty());
        data.forEach(row -> {
            Assert.assertTrue(row.containsKey("Score"), JsonUtils.serialize(row));
            String score = (String) row.get("Score");
            Assert.assertNotNull(score);
            Assert.assertTrue(
                    Arrays.asList(RatingBucketName.A.getName(), RatingBucketName.C.getName()).contains(score));
        });

        frontEndQuery.setLookups(Arrays.asList( //
                new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name()), //
                new AttributeLookup(BusinessEntity.Rating, model.getId())) //
        );
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        dataPage = ratingQueryService.getData(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertNotNull(dataPage);
        data = dataPage.getData();
        Assert.assertFalse(data.isEmpty());
        data.forEach(row -> {
            Assert.assertTrue(row.containsKey(model.getId()));
            String score = (String) row.get(model.getId());
            Assert.assertNotNull(score);
            Assert.assertTrue(
                    Arrays.asList(RatingBucketName.A.getName(), RatingBucketName.C.getName()).contains(score));
        });

        // only get scores for A, B and FStatsCubeUtils
        Restriction selectedScores = Restriction.builder().let(BusinessEntity.Rating, model.getId()).inCollection(
                Arrays.asList(RatingBucketName.A.getName(), RatingBucketName.B.getName(), RatingBucketName.F.getName()))
                .build();
        Restriction restriction2 = Restriction.builder().and(restriction, selectedScores).build();
        frontEndRestriction.setRestriction(restriction2);
        frontEndQuery.setAccountRestriction(frontEndRestriction);

        dataPage = ratingQueryService.getData(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertNotNull(dataPage);
        data = dataPage.getData();
        Assert.assertFalse(data.isEmpty());
        data.forEach(row -> {
            Assert.assertTrue(row.containsKey(model.getId()));
            String score = (String) row.get(model.getId());
            Assert.assertEquals(score, RatingBucketName.A.getName());
        });
    }

    @Test(groups = "functional")
    public void testScoreCount() {
        RatingModel model = ruleBasedModel();

        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction accountRestriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("A")
                .build();
        frontEndRestriction.setRestriction(accountRestriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);

        Bucket contactBkt = Bucket.valueBkt(ComparisonType.CONTAINS, Collections.singletonList(VALUE_CONTACT_TITLE));
        Restriction contactRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Contact, ATTR_CONTACT_TITLE), contactBkt);
        frontEndQuery.setContactRestriction(new FrontEndRestriction(contactRestriction));

        frontEndQuery.setRatingModels(Collections.singletonList(model));
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setSort(new FrontEndSort(
                Collections.singletonList(new AttributeLookup(BusinessEntity.Account, ATTR_ACCOUNT_NAME)), false));

        Map<String, Long> ratingCounts = ratingQueryService.getRatingCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertNotNull(ratingCounts);
        Assert.assertFalse(ratingCounts.isEmpty());
        ratingCounts.forEach((score, count) -> {
            if (RatingBucketName.A.getName().equals(score)) {
                Assert.assertEquals(count, Long.valueOf(1687));
            } else if (RatingBucketName.C.getName().equals(score)) {
                Assert.assertEquals(count, Long.valueOf(371));
            }
        });
    }

    @Test(groups = "functional")
    public void testScoreCountSummation() {
        RatingModel model = ruleBasedModel();

        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);

        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction accountRestriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("A")
                .build();
        frontEndRestriction.setRestriction(accountRestriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);

        long totalCount = ratingQueryService.getCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);

        frontEndQuery.setRatingModels(Collections.singletonList(model));
        frontEndQuery.setSort(new FrontEndSort(
                Collections.singletonList(new AttributeLookup(BusinessEntity.Account, ATTR_ACCOUNT_NAME)), false));

        Map<String, Long> ratingCounts = ratingQueryService.getRatingCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertNotNull(ratingCounts);
        Assert.assertFalse(ratingCounts.isEmpty());
        AtomicLong sumAgg = new AtomicLong(0L);
        ratingCounts.values().forEach(sumAgg::addAndGet);
        long sum = sumAgg.get();

        Assert.assertEquals(sum, totalCount,
                String.format("Sum of rating coverage %d does not equals to the segment count %d", sum, totalCount));
    }

    @Test(groups = "functional")
    public void testRuleModelWithTxn() {
        RatingModel model = ruleBasedModelWithTxn();
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        frontEndQuery.setRatingModels(Collections.singletonList(model));
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setSort(new FrontEndSort(
                Collections.singletonList(new AttributeLookup(BusinessEntity.Account, ATTR_ACCOUNT_NAME)), false));
        Map<String, Long> ratingCounts = ratingQueryService.getRatingCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertNotNull(ratingCounts);
        Assert.assertFalse(ratingCounts.isEmpty());
        ratingCounts.forEach((score, count) -> {
            if (RatingBucketName.A.getName().equals(score)) {
                Assert.assertEquals(count, Long.valueOf(3908));
            } else if (RatingBucketName.C.getName().equals(score)) {
                Assert.assertEquals(count, Long.valueOf(29340));
            }
        });
    }

    @Test(groups = "functional")
    public void testRuleModelWithEncodedAttrIsNull() {
        RuleBasedModel model = new RuleBasedModel();
        model.setId(UuidUtils.shortenUuid(UUID.randomUUID()));
        RatingRule rule = RatingRule.constructDefaultRule();
        Map<String, Restriction> ruleA = new HashMap<>();
        ruleA.put(FrontEndQueryConstants.ACCOUNT_RESTRICTION, Restriction.builder().and( //
                new BucketRestriction(BusinessEntity.Account, "BmbrSurge_AdNetworks_Intent", Bucket.nullBkt())
        ).build());
        rule.getBucketToRuleMap().put(RatingBucketName.A.getName(), ruleA);
        rule.setDefaultBucketName(RatingBucketName.C.getName());
        model.setRatingRule(rule);

        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        frontEndQuery.setRatingModels(Collections.singletonList(model));
        frontEndQuery.setMainEntity(BusinessEntity.Account);

        frontEndQuery.addLookups(BusinessEntity.Account, InterfaceName.AccountId.name());
        frontEndQuery.addLookups(BusinessEntity.Rating, model.getId());

        DataPage dataPage = ratingQueryService.getData(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertNotNull(dataPage);
        List<Map<String, Object>> data = dataPage.getData();
        Assert.assertFalse(data.isEmpty());
        data.forEach(row -> {
            Assert.assertFalse(row.containsKey("Score"), JsonUtils.serialize(row));
            Assert.assertTrue(row.containsKey(model.getId()), JsonUtils.serialize(row));
        });
    }

    private RuleBasedModel ruleBasedModel() {
        RuleBasedModel model = new RuleBasedModel();
        model.setId(UuidUtils.shortenUuid(UUID.randomUUID()));
        RatingRule rule = RatingRule.constructDefaultRule();

        for (RatingBucketName bucket: RatingBucketName.values()) {
            rule.getBucketToRuleMap().put(bucket.getName(), defaultRulesFromUI());
        }

        Map<String, Restriction> ruleA = new HashMap<>();
        ruleA.put(FrontEndQueryConstants.ACCOUNT_RESTRICTION, Restriction.builder().and(
                new BucketRestriction(BusinessEntity.Account, ATTR_ACCOUNT_NAME, Bucket.rangeBkt("B", "G")),
                new BucketRestriction(BusinessEntity.Account, ATTR_ACCOUNT_NAME, Bucket.notNullBkt())
        ).build());
        ruleA.put(FrontEndQueryConstants.CONTACT_RESTRICTION,
                new BucketRestriction(BusinessEntity.Contact, ATTR_CONTACT_TITLE, Bucket.rangeBkt("A", "N")));
        rule.getBucketToRuleMap().put(RatingBucketName.A.getName(), ruleA);

        Map<String, Restriction> ruleC = new HashMap<>();
        ruleC.put(FrontEndQueryConstants.ACCOUNT_RESTRICTION,
                new BucketRestriction(BusinessEntity.Account, ATTR_ACCOUNT_NAME, Bucket.rangeBkt("H", "N")));
        ruleC.put(FrontEndQueryConstants.CONTACT_RESTRICTION,
                new BucketRestriction(BusinessEntity.Contact, ATTR_CONTACT_TITLE, Bucket.rangeBkt("A", "N")));
        rule.getBucketToRuleMap().put(RatingBucketName.C.getName(), ruleC);

        rule.setDefaultBucketName(RatingBucketName.A.getName());

        model.setRatingRule(rule);

        return model;
    }

    private static Map<String, Restriction> defaultRulesFromUI() {
        Map<String, Restriction> map = new HashMap<>();
        map.put(FrontEndQueryConstants.ACCOUNT_RESTRICTION, LogicalRestriction.builder().and(Collections.emptyList()).build());
        map.put(FrontEndQueryConstants.CONTACT_RESTRICTION, LogicalRestriction.builder().and(Collections.emptyList()).build());
        return map;
    }

    @Test(groups = "functional")
    public void testScoreDataWithEmptyRuleMap() {
        RuleBasedModel model = new RuleBasedModel();
        model.setId(UuidUtils.shortenUuid(UUID.randomUUID()));
        RatingRule rule = RatingRule.constructDefaultRule();
        rule.setDefaultBucketName(RatingBucketName.C.getName());
        model.setRatingRule(rule);

        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        frontEndQuery.setRatingModels(Collections.singletonList(model));
        frontEndQuery.setPageFilter(new PageFilter(0, 10));
        frontEndQuery.setSort(new FrontEndSort(
                Collections.singletonList(new AttributeLookup(BusinessEntity.Account, ATTR_ACCOUNT_NAME)), false));

        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction restriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("D").build();
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);

        Long count = ratingQueryService.getCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertNotNull(count);

        Map<String, Long> coverage = ratingQueryService.getRatingCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertTrue(MapUtils.isNotEmpty(coverage));
        Assert.assertEquals(coverage.size(), 1);
        Assert.assertTrue(coverage.keySet().contains(RatingBucketName.C.name()));
        Assert.assertEquals(coverage.get(RatingBucketName.C.name()), count);

        DataPage dataPage = ratingQueryService.getData(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertNotNull(dataPage);
        List<Map<String, Object>> data = dataPage.getData();
        Assert.assertFalse(data.isEmpty());
        data.forEach(row -> {
            Assert.assertTrue(row.containsKey("Score"));
            String score = (String) row.get("Score");
            Assert.assertNotNull(score);
            Assert.assertEquals(score, RatingBucketName.C.name());
        });
    }

    private RuleBasedModel ruleBasedModelWithTxn() {
        RuleBasedModel model = new RuleBasedModel();
        model.setId(UuidUtils.shortenUuid(UUID.randomUUID()));
        RatingRule rule = RatingRule.constructDefaultRule();

        Map<String, Restriction> ruleA = new HashMap<>();
        String prodId = "o13brsbfF10fllM6VUZRxMO7wfo5I7Ks";
        TimeFilter timeFilter = TimeFilter.within(2, "Month");
        Bucket.Transaction txn = new Bucket.Transaction(prodId, timeFilter, null, null, false);
        ruleA.put(FrontEndQueryConstants.ACCOUNT_RESTRICTION,
                new BucketRestriction(BusinessEntity.PurchaseHistory, "HasPurchased", Bucket.txnBkt(txn)));
        rule.getBucketToRuleMap().put(RatingBucketName.A.getName(), ruleA);

        rule.setDefaultBucketName(RatingBucketName.C.getName());

        model.setRatingRule(rule);

        return model;
    }

}
