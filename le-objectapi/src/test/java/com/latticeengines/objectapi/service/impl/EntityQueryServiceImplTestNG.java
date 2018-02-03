package com.latticeengines.objectapi.service.impl;

import static org.mockito.ArgumentMatchers.any;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQueryConstants;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndSort;
import com.latticeengines.domain.exposed.query.util.ExpressionTemplateUtils;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.objectapi.functionalframework.ObjectApiFunctionalTestNGBase;
import com.latticeengines.objectapi.service.EntityQueryService;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;
import com.latticeengines.query.exposed.translator.TransactionRestrictionTranslator;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class EntityQueryServiceImplTestNG extends ObjectApiFunctionalTestNGBase {

    private static final String ATTR_ACCOUNT_NAME = "name";
    private static final String ATTR_CONTACT_TITLE = "Title";

    @Autowired
    private EntityQueryService entityQueryService;

    @Autowired
    private QueryEvaluatorService queryEvaluatorService;

    @BeforeClass(groups = "functional")
    public void setup() {
        mockDataCollectionProxy();
        MultiTenantContext.setTenant(new Tenant("LocalTest"));
        String maxTransactionDate = getMaxTransactionDate();
        ExpressionTemplateUtils.setCurrentDate(String.format("'%s'", maxTransactionDate));
        TransactionRestrictionTranslator.setCurrentDate(String.format("'%s'", maxTransactionDate));
    }

    @Test(groups = "functional")
    public void testProductCountAndData() {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction restriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("A").build();
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Product);

        Long count = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue);
        Assert.assertNotNull(count);
        Assert.assertEquals(count, new Long(149L));

        DataPage dataPage = entityQueryService.getData(frontEndQuery, DataCollection.Version.Blue);
        for (Map<String, Object> product : dataPage.getData()) {
            Assert.assertTrue(product.containsKey(InterfaceName.ProductId.name()));
            Assert.assertTrue(product.containsKey(InterfaceName.ProductName.name()));
        }
    }

    @Test(groups = "functional")
    public void testAccountCount() {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction restriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("A").build();
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        Long count = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue);
        Assert.assertNotNull(count);
        Assert.assertEquals(count, new Long(3165L));

        // must have transaction
        frontEndQuery.setRestrictHasTransaction(true);
        count = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue);
        Assert.assertNotNull(count);
        Assert.assertEquals(count, new Long(1329L));
    }

    @Test(groups = "functional", dataProvider = "timefilterProvider")
    public void testAccountWithTxn(TimeFilter timeFilter, long expectedTotal) {
        MultiTenantContext.setTenant(new Tenant("LocalTest"));
        String prodId = "6368494B622E0CB60F9C80FEB1D0F95F";

        // Ever Purchased
        Bucket.Transaction txn = new Bucket.Transaction(prodId, timeFilter, null, null, false);
        long totalCount = countTxnBkt(txn);
        Assert.assertEquals(totalCount, expectedTotal);

        // Ever, Amount > 0, Quantity > 0
        AggregationFilter greaterThan0 = new AggregationFilter(ComparisonType.GREATER_THAN, Collections.singletonList(0));
        txn = new Bucket.Transaction(prodId, timeFilter, greaterThan0, greaterThan0, false);
        long count = countTxnBkt(txn);
        Assert.assertEquals(count, totalCount);

        // Check add up
        AggregationFilter filter1 = new AggregationFilter(ComparisonType.GT_AND_LT, Arrays.asList(0, 1000));
        AggregationFilter filter2 = new AggregationFilter(ComparisonType.GTE_AND_LT, Arrays.asList(1000, 3000));
        AggregationFilter filter3 = new AggregationFilter(ComparisonType.GREATER_OR_EQUAL, Collections.singletonList(3000));
        Bucket.Transaction txn1 = new Bucket.Transaction(prodId, timeFilter, filter1, null, false);
        Bucket.Transaction txn2 = new Bucket.Transaction(prodId, timeFilter, filter2, null, false);
        Bucket.Transaction txn3 = new Bucket.Transaction(prodId, timeFilter, filter3, null, false);
        long count1 = countTxnBkt(txn1);
        long count2 = countTxnBkt(txn2);
        long count3 = countTxnBkt(txn3);
        Assert.assertEquals(count1 + count2 + count3, totalCount);

        filter1 = new AggregationFilter(ComparisonType.GT_AND_LT, Arrays.asList(0, 1));
        filter2 = new AggregationFilter(ComparisonType.GTE_AND_LT, Arrays.asList(1, 10));
        filter3 = new AggregationFilter(ComparisonType.GREATER_OR_EQUAL, Collections.singletonList(10));
        txn1 = new Bucket.Transaction(prodId, timeFilter, filter1, null, false);
        txn2 = new Bucket.Transaction(prodId, timeFilter, filter2, null, false);
        txn3 = new Bucket.Transaction(prodId, timeFilter, filter3, null, false);
        count1 = countTxnBkt(txn1);
        count2 = countTxnBkt(txn2);
        count3 = countTxnBkt(txn3);
        Assert.assertEquals(count1 + count2 + count3, totalCount);
    }

    @DataProvider(name = "timefilterProvider", parallel = true)
    public Object[][] timefilterProvider() {
        TimeFilter currentMonth = new TimeFilter( //
                ComparisonType.IN_CURRENT_PERIOD, //
                TimeFilter.Period.Month.name(), //
                Collections.emptyList());
        TimeFilter lastMonth = new TimeFilter( //
                ComparisonType.EQUAL, //
                TimeFilter.Period.Month.name(), //
                Collections.singletonList(1));
        return new Object[][] {
                { TimeFilter.ever(), 832L }, //
                { currentMonth, 223L }, //
                { lastMonth, 218L },
        };
    }

    private String getMaxTransactionDate() {
        AttributeLookup lookup = new AttributeLookup(BusinessEntity.Transaction, InterfaceName.TransactionDate.name());
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setLookups(Collections.singletonList(lookup));
        frontEndQuery.setSort(new FrontEndSort(Collections.singletonList(lookup), true));
        frontEndQuery.setPageFilter(new PageFilter(0, 1));
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        DataPage dataPage = entityQueryService.getData(frontEndQuery, DataCollection.Version.Blue);
        Assert.assertNotNull(dataPage);
        return (String) dataPage.getData().get(0).get(InterfaceName.TransactionDate.name());
    }

    @Test(groups = "functional")
    public void testAccountContactWithTxn() {
        String prodId = "6368494B622E0CB60F9C80FEB1D0F95F";
        // Check add up
        AggregationFilter filter = new AggregationFilter(ComparisonType.GT_AND_LT, Arrays.asList(0, 1000));
        Bucket.Transaction txn = new Bucket.Transaction(prodId, TimeFilter.ever(), filter, null, false);

        Restriction txnRestriction = getTxnRestriction(txn);
        Restriction accRestriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("A").build();
        Restriction cntRestriction = Restriction.builder().let(BusinessEntity.Contact, InterfaceName.Title.name()).eq("Buyer").build();

        FrontEndQuery frontEndQuery = new FrontEndQuery();

        FrontEndRestriction accountFERestriction = new FrontEndRestriction();
        accountFERestriction.setRestriction(Restriction.builder().and(txnRestriction, accRestriction).build());
        frontEndQuery.setAccountRestriction(accountFERestriction);

        FrontEndRestriction contactFERestriction = new FrontEndRestriction();
        contactFERestriction.setRestriction(cntRestriction);
        frontEndQuery.setContactRestriction(contactFERestriction);

        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setRestrictHasTransaction(true);
        Long count = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue);
        Assert.assertNotNull(count);
        Assert.assertEquals(count, new Long(43));
    }

    @Test(groups = "functional")
    public void testAccountContactWithDeletedTxn() {
        String prodId = "6368494B622E0CB60F9C80FEB1D0F95F";
        AggregationFilter filter = new AggregationFilter(ComparisonType.GT_AND_LT, Arrays.asList(0, 1000));
        Bucket.Transaction txn = new Bucket.Transaction(prodId, TimeFilter.ever(), filter, null, false);
        BucketRestriction txnRestriction = (BucketRestriction) getTxnRestriction(txn);
        txnRestriction.setDeleted(true);

        Restriction accRestriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("A").build();
        Restriction cntRestriction = Restriction.builder().let(BusinessEntity.Contact, InterfaceName.Title.name()).eq("Buyer").build();

        FrontEndQuery frontEndQuery = new FrontEndQuery();

        FrontEndRestriction accountFERestriction = new FrontEndRestriction();
        accountFERestriction.setRestriction(Restriction.builder().and(txnRestriction, accRestriction).build());
        frontEndQuery.setAccountRestriction(accountFERestriction);

        FrontEndRestriction contactFERestriction = new FrontEndRestriction();
        contactFERestriction.setRestriction(cntRestriction);
        frontEndQuery.setContactRestriction(contactFERestriction);

        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setRestrictHasTransaction(true);
        Long count = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue);
        Assert.assertNotNull(count);
        Assert.assertEquals(count, new Long(178));
    }

    private long countTxnBkt(Bucket.Transaction txn) {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction restriction = getTxnRestriction(txn);
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        Long count = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue);
        Assert.assertNotNull(count);
        return count;
    }

    private Restriction getTxnRestriction(Bucket.Transaction txn) {
        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.Transaction, "AnyThing");
        Bucket bucket = Bucket.txnBkt(txn);
        return new BucketRestriction(attrLookup, bucket);
    }

    @Test(groups = "functional")
    public void testContactAccountWithTxn() {
        String prodId = "6368494B622E0CB60F9C80FEB1D0F95F";
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        FrontEndRestriction frontEndRestriction1 = new FrontEndRestriction();
        Restriction restriction1 = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("A")
                .build();
        Bucket.Transaction txn = new Bucket.Transaction(prodId, TimeFilter.ever(), null, null, false);
        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.Transaction, "AnyThing");
        Bucket bucket = Bucket.txnBkt(txn);
        Restriction txRestriction = new BucketRestriction(attrLookup, bucket);
        Restriction logicalRestriction = Restriction.builder().and(restriction1, txRestriction).build();
        frontEndRestriction1.setRestriction(logicalRestriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction1);

        FrontEndRestriction frontEndRestriction2 = new FrontEndRestriction();
        Restriction restriction2 = Restriction.builder().let(BusinessEntity.Contact, ATTR_CONTACT_TITLE).gte("VP")
                .build();
        frontEndRestriction2.setRestriction(restriction2);
        frontEndQuery.setContactRestriction(frontEndRestriction2);

        frontEndQuery.setMainEntity(BusinessEntity.Contact);
        Long count = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue);
        Assert.assertNotNull(count);
        Assert.assertEquals(count, new Long(15L));

    }

    @Test(groups = "functional")
    public void testAccountContactCount() {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        FrontEndRestriction frontEndRestriction1 = new FrontEndRestriction();
        Restriction restriction1 = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("A")
                .build();
        frontEndRestriction1.setRestriction(restriction1);
        frontEndQuery.setAccountRestriction(frontEndRestriction1);

        FrontEndRestriction frontEndRestriction2 = new FrontEndRestriction();
        Restriction restriction2 = Restriction.builder().let(BusinessEntity.Contact, ATTR_CONTACT_TITLE).gte("VP")
                .build();
        frontEndRestriction2.setRestriction(restriction2);
        frontEndQuery.setContactRestriction(frontEndRestriction2);

        frontEndQuery.setMainEntity(BusinessEntity.Account);
        Long count = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue);
        Assert.assertNotNull(count);
        Assert.assertEquals(count, new Long(43L));

        frontEndQuery.setMainEntity(BusinessEntity.Contact);
        count = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue);
        Assert.assertNotNull(count);
        Assert.assertEquals(count, new Long(44L));
    }

    @Test(groups = "functional")
    public void testContactDataWithAccountIds() {
        // mimic a segment get from metadata api
        MetadataSegment segment = new MetadataSegment();
        Restriction accountRestrictionInternal = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME)
                .gte("A").build();
        segment.setAccountRestriction(accountRestrictionInternal);
        Restriction contactRestrictionInternal = Restriction.builder().let(BusinessEntity.Contact, ATTR_CONTACT_TITLE)
                .gte("VP").build();
        segment.setContactRestriction(contactRestrictionInternal);

        // front end query from segment restrictions
        FrontEndQuery queryFromSegment = FrontEndQuery.fromSegment(segment);

        // get account ids first
        queryFromSegment.setMainEntity(BusinessEntity.Account);
        queryFromSegment.setLookups( //
                Collections.singletonList(new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name())));
        queryFromSegment.setSort(new FrontEndSort(
                Collections.singletonList(new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name())),
                false));
        queryFromSegment.setPageFilter(new PageFilter(0, 10));
        DataPage dataPage = entityQueryService.getData(queryFromSegment, DataCollection.Version.Blue);
        List<Object> accountIds = dataPage.getData().stream().map(m -> m.get(InterfaceName.AccountId.name())) //
                .collect(Collectors.toList());
        Assert.assertEquals(accountIds.size(), 10);

        // extract contact restriction
        Restriction extractedContactRestriction = queryFromSegment.getContactRestriction().getRestriction();
        Restriction accountIdRestriction = Restriction.builder()
                .let(BusinessEntity.Contact, InterfaceName.AccountId.name()).inCollection(accountIds).build();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction(
                Restriction.builder().and(extractedContactRestriction, accountIdRestriction).build());
        FrontEndQuery contactQuery = new FrontEndQuery();
        contactQuery.setMainEntity(BusinessEntity.Contact);
        contactQuery.setContactRestriction(frontEndRestriction);
        contactQuery.setLookups(Arrays.asList( //
                new AttributeLookup(BusinessEntity.Contact, InterfaceName.AccountId.name()),
                new AttributeLookup(BusinessEntity.Contact, InterfaceName.ContactId.name())));
        contactQuery.setPageFilter(new PageFilter(0, 100));
        dataPage = entityQueryService.getData(contactQuery, DataCollection.Version.Blue);
        Assert.assertEquals(dataPage.getData().size(), 10);
        for (Map<String, Object> contact : dataPage.getData()) {
            Object accountId = contact.get(InterfaceName.AccountId.name());
            Assert.assertTrue(accountIds.contains(accountId));
        }
        FrontEndQuery emptyContactQuery = new FrontEndQuery();
        emptyContactQuery.setMainEntity(BusinessEntity.Contact);
        emptyContactQuery.setContactRestriction(frontEndRestriction);
        dataPage = entityQueryService.getData(emptyContactQuery, DataCollection.Version.Blue);
        Assert.assertEquals(dataPage.getData().size(), 10);
        for (Map<String, Object> contact : dataPage.getData()) {
            Assert.assertTrue(contact.containsKey(InterfaceName.Email.toString()));
        }
    }

    @Test(groups = "functional")
    public void testLogicalOr() {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction restriction1 = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).contains("On").build();
        Restriction restriction2 = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).contains("Ca").build();
        frontEndRestriction.setRestriction(Restriction.builder().or(restriction1, restriction2).build());
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setRestrictHasTransaction(true);

        long count = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue);
    }

    @Test(groups = "functional")
    public void testScoreData() {
        RatingModel model = ruleBasedModel();

        FrontEndQuery frontEndQuery = new FrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction restriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("D").build();
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        Bucket contactBkt = Bucket.valueBkt(ComparisonType.CONTAINS, Collections.singletonList("Manager"));
        Restriction contactRestriction = new BucketRestriction(new AttributeLookup(BusinessEntity.Contact, ATTR_CONTACT_TITLE), contactBkt);
        frontEndQuery.setContactRestriction(new FrontEndRestriction(contactRestriction));
        frontEndQuery.setRatingModels(Collections.singletonList(model));
        frontEndQuery.setPageFilter(new PageFilter(0, 10));
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setSort(new FrontEndSort(
                Collections.singletonList(new AttributeLookup(BusinessEntity.Account, ATTR_ACCOUNT_NAME)), false));

        DataPage dataPage = entityQueryService.getData(frontEndQuery, DataCollection.Version.Blue);
        Assert.assertNotNull(dataPage);
        List<Map<String, Object>> data = dataPage.getData();
        Assert.assertFalse(data.isEmpty());
        data.forEach(row -> {
            Assert.assertTrue(row.containsKey("Score"));
            String score = (String) row.get("Score");
            Assert.assertNotNull(score);
            Assert.assertTrue(Arrays.asList(RatingBucketName.A.getName(), RatingBucketName.C.getName()).contains(score));
        });

        frontEndQuery.setLookups(Arrays.asList(new AttributeLookup(BusinessEntity.Account, "AccountId"),
                new AttributeLookup(BusinessEntity.Rating, model.getId())));
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        dataPage = entityQueryService.getData(frontEndQuery, DataCollection.Version.Blue);
        Assert.assertNotNull(dataPage);
        data = dataPage.getData();
        Assert.assertFalse(data.isEmpty());
        data.forEach(row -> {
            Assert.assertTrue(row.containsKey(model.getId()));
            String score = (String) row.get(model.getId());
            Assert.assertNotNull(score);
            Assert.assertTrue(Arrays.asList(RatingBucketName.A.getName(), RatingBucketName.C.getName()).contains(score));
        });

        // only get scores for A, B and FStatsCubeUtils
        Restriction selectedScores = Restriction.builder().let(BusinessEntity.Rating, model.getId()).inCollection(
                Arrays.asList(RatingBucketName.A.getName(), RatingBucketName.B.getName(), RatingBucketName.F.getName()))
                .build();
        Restriction restriction2 = Restriction.builder().and(restriction, selectedScores).build();
        frontEndRestriction.setRestriction(restriction2);
        frontEndQuery.setAccountRestriction(frontEndRestriction);

        dataPage = entityQueryService.getData(frontEndQuery, DataCollection.Version.Blue);
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
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction accountRestriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("A").build();
        frontEndRestriction.setRestriction(accountRestriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);

        Bucket contactBkt = Bucket.valueBkt(ComparisonType.CONTAINS, Collections.singletonList("Manager"));
        Restriction contactRestriction = new BucketRestriction(new AttributeLookup(BusinessEntity.Contact, ATTR_CONTACT_TITLE), contactBkt);
        frontEndQuery.setContactRestriction(new FrontEndRestriction(contactRestriction));

        frontEndQuery.setRatingModels(Collections.singletonList(model));
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setSort(new FrontEndSort(
                Collections.singletonList(new AttributeLookup(BusinessEntity.Account, ATTR_ACCOUNT_NAME)), false));

        Map<String, Long> ratingCounts = entityQueryService.getRatingCount(frontEndQuery, DataCollection.Version.Blue);
        Assert.assertNotNull(ratingCounts);
        Assert.assertFalse(ratingCounts.isEmpty());
        ratingCounts.forEach((score, count) -> {
            if (RatingBucketName.A.getName().equals(score)) {
                Assert.assertEquals((long) count, 483L);
            } else if (RatingBucketName.C.getName().equals(score)) {
                Assert.assertEquals((long) count, 99L);
            }
        });
    }

    private void mockDataCollectionProxy() {
        DataCollectionProxy proxy = Mockito.mock(DataCollectionProxy.class);
        Mockito.when(proxy.getAttrRepo(any(), any())).thenReturn(attrRepo);
        queryEvaluatorService.setDataCollectionProxy(proxy);
    }

    private RuleBasedModel ruleBasedModel() {
        RuleBasedModel model = new RuleBasedModel();
        model.setId(UuidUtils.shortenUuid(UUID.randomUUID()));
        RatingRule rule = RatingRule.constructDefaultRule();

        Map<String, Restriction> ruleA = new HashMap<>();
        ruleA.put(FrontEndQueryConstants.ACCOUNT_RESTRICTION,
                Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).in("B", "G").build());
        ruleA.put(FrontEndQueryConstants.CONTACT_RESTRICTION,
                Restriction.builder().let(BusinessEntity.Contact, ATTR_CONTACT_TITLE).in("A", "N").build());
        rule.getBucketToRuleMap().put(RatingBucketName.A.getName(), ruleA);

        Map<String, Restriction> ruleC = new HashMap<>();
        ruleC.put(FrontEndQueryConstants.ACCOUNT_RESTRICTION,
                Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).in("H", "N").build());
        rule.getBucketToRuleMap().put(RatingBucketName.C.getName(), ruleC);

        rule.setDefaultBucketName(RatingBucketName.A.getName());

        model.setRatingRule(rule);

        return model;
    }

}
