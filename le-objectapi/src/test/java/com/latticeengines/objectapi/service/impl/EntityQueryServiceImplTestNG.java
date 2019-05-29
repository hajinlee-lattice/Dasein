package com.latticeengines.objectapi.service.impl;

import static com.latticeengines.domain.exposed.util.RestrictionUtils.TRANSACTION_LOOKUP;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndSort;
import com.latticeengines.domain.exposed.query.frontend.RatingEngineFrontEndQuery;
import com.latticeengines.objectapi.service.EntityQueryService;
import com.latticeengines.objectapi.service.EventQueryService;

public class EntityQueryServiceImplTestNG extends QueryServiceImplTestNGBase {

    private final class AccountAttr {
        static final String CompanyName = "CompanyName";
    }

    private final class ContactAttr {
        static final String Occupation = "Occupation";
    }

    @Inject
    private EntityQueryService entityQueryService;

    @Inject
    private EventQueryService eventQueryService;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestData(3);
    }

    @DataProvider(name = "userContexts", parallel = false)
    private Object[][] provideSqlUserContexts() {
        return new Object[][] { { SEGMENT_USER, "Redshift" } };
    }

    protected EntityQueryService getEntityQueryService(String sqlUser) {
        return entityQueryService;
    }

    protected Object[][] getTimeFilterDataProvider() {
        TimeFilter currentMonth = new TimeFilter( //
                ComparisonType.IN_CURRENT_PERIOD, //
                PeriodStrategy.Template.Month.name(), //
                Collections.emptyList());
        TimeFilter lastMonth = new TimeFilter( //
                ComparisonType.WITHIN, //
                PeriodStrategy.Template.Month.name(), //
                Collections.singletonList(1));
        TimeFilter betweendates = new TimeFilter( //
                ComparisonType.BETWEEN_DATE, //
                PeriodStrategy.Template.Date.name(), //
                Arrays.asList("2017-07-15", "2017-08-15"));
        return new Object[][] { //
                { SEGMENT_USER, TimeFilter.ever(), 24636L }, //
                { SEGMENT_USER, currentMonth, 1983L }, //
                { SEGMENT_USER, lastMonth, 2056L }, //
                { SEGMENT_USER, betweendates, 2185L }, //
        };
    }

    @SuppressWarnings("unused")
    private long compareTgtCount(Bucket.Transaction txn) {
        EventFrontEndQuery frontEndQuery = new EventFrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction restriction = getTxnRestriction(txn);
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setTargetProductIds(Collections.singletonList("o13brsbfF10fllM6VUZRxMO7wfo5I7Ks"));
        return eventQueryService.getScoringCount(frontEndQuery, DataCollection.Version.Blue);
    }

    private Restriction getTxnRestriction(Bucket.Transaction txn) {
        Bucket bucket = Bucket.txnBkt(txn);
        return new BucketRestriction(TRANSACTION_LOOKUP, bucket);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testDistinct(String sqlUser, String queryContext) {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        frontEndQuery.addLookups(BusinessEntity.Product, InterfaceName.ProductId.name());
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Product);
        frontEndQuery.setDistinct(true);

        DataPage dataPage = getEntityQueryService(sqlUser).getData(frontEndQuery, DataCollection.Version.Blue, //
                sqlUser, true);
        Assert.assertNotNull(dataPage);
        Assert.assertNotNull(dataPage.getData());
        testAndAssertCount(sqlUser, dataPage.getData().size(), 10L);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testProductCountAndData(String sqlUser, String queryContext) {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction restriction = Restriction.builder().let(BusinessEntity.Account, AccountAttr.CompanyName).gte("A")
                .build();
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Product);

        long count = getEntityQueryService(sqlUser).getCount(frontEndQuery, DataCollection.Version.Blue, sqlUser);
        testAndAssertCount(sqlUser, count, 10);

        DataPage dataPage = getEntityQueryService(sqlUser).getData(frontEndQuery, DataCollection.Version.Blue, sqlUser,
                false);
        for (Map<String, Object> product : dataPage.getData()) {
            Assert.assertTrue(product.containsKey(InterfaceName.ProductId.name()));
            Assert.assertTrue(product.containsKey(InterfaceName.ProductName.name()));
        }
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAccountCount(String sqlUser, String queryContext) {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction restriction = Restriction.builder().let(BusinessEntity.Account, AccountAttr.CompanyName).gte("A")
                .build();
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        long count = getEntityQueryService(sqlUser).getCount(frontEndQuery, DataCollection.Version.Blue, sqlUser);
        testAndAssertCount(sqlUser, count, 32802);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testContactCount(String sqlUser, String queryContext) {
        FrontEndQuery frontEndQuery = JsonUtils.deserialize("{\"main_entity\": \"Contact\",  \"distinct\": false}",
                FrontEndQuery.class);
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        frontEndQuery.setMainEntity(BusinessEntity.Contact);
        long count = getEntityQueryService(sqlUser).getCount(frontEndQuery, DataCollection.Version.Blue, sqlUser);
        Assert.assertEquals(count, 132658);
        testAndAssertCount(sqlUser, count, 132658);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testMetricRestriction(String sqlUser, String queryContext) {
        final String phAttr = "AM_uKt9Tnd4sTXNUxEMzvIXcC9eSkaGah8__M_1_6__AS";
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Bucket bucket = Bucket.chgBkt(Bucket.Change.Direction.DEC, Bucket.Change.ComparisonType.AS_MUCH_AS,
                Collections.singletonList(5));
        Restriction restriction = new BucketRestriction(BusinessEntity.PurchaseHistory, phAttr, bucket);
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.addLookups(BusinessEntity.PurchaseHistory, phAttr);
        long count = getEntityQueryService(sqlUser).getCount(frontEndQuery, DataCollection.Version.Blue, sqlUser);
        Assert.assertEquals(count, 33248);
        testAndAssertCount(sqlUser, count, 33248);
        // testGetDataForMetricRestriction(sqlUser, phAttr, frontEndQuery);
    }

    private void testGetDataForMetricRestriction(String sqlUser, final String phAttr, FrontEndQuery frontEndQuery) {
        frontEndQuery.setPageFilter(new PageFilter(0, 10));
        DataPage dataPage = getEntityQueryService(sqlUser).getData(frontEndQuery, DataCollection.Version.Blue, sqlUser,
                false);
        Assert.assertNotNull(dataPage);
        Assert.assertTrue(CollectionUtils.isNotEmpty(dataPage.getData()));
        // not support querying PH attrs
        Assert.assertFalse(dataPage.getData().get(0).containsKey(phAttr));
    }

    @Test(groups = "functional", dataProvider = "timefilterProvider")
    public void testAccountWithTxn(String sqlUser, TimeFilter timeFilter, long expectedTotal) {
        MultiTenantContext.setTenant(tenant);
        String prodId = "o13brsbfF10fllM6VUZRxMO7wfo5I7Ks";

        // Ever Purchased
        Bucket.Transaction txn = new Bucket.Transaction(prodId, timeFilter, null, null, false);
        long totalCount = countTxnBkt(txn, sqlUser);
        testAndAssertCount(sqlUser, totalCount, expectedTotal);

        // Ever, Amount > 0, Quantity > 0
        AggregationFilter greaterThan0 = new AggregationFilter(ComparisonType.GREATER_THAN,
                Collections.singletonList(0));
        txn = new Bucket.Transaction(prodId, timeFilter, greaterThan0, greaterThan0, false);
        long count = countTxnBkt(txn, sqlUser);
        Assert.assertTrue(count <= totalCount, String.format("%d <= %d", count, totalCount));

        // Check add up
        AggregationFilter filter1 = new AggregationFilter(ComparisonType.GT_AND_LT, Arrays.asList(0, 1000));
        AggregationFilter filter2 = new AggregationFilter(ComparisonType.GTE_AND_LT, Arrays.asList(1000, 3000));
        AggregationFilter filter3 = new AggregationFilter(ComparisonType.GREATER_OR_EQUAL,
                Collections.singletonList(3000));
        Bucket.Transaction txn1 = new Bucket.Transaction(prodId, timeFilter, filter1, null, false);
        Bucket.Transaction txn2 = new Bucket.Transaction(prodId, timeFilter, filter2, null, false);
        Bucket.Transaction txn3 = new Bucket.Transaction(prodId, timeFilter, filter3, null, false);
        long count1 = countTxnBkt(txn1, sqlUser);
        long count2 = countTxnBkt(txn2, sqlUser);
        long count3 = countTxnBkt(txn3, sqlUser);
        Assert.assertEquals(count1 + count2 + count3, count);

        filter1 = new AggregationFilter(ComparisonType.GT_AND_LT, Arrays.asList(0, 1));
        filter2 = new AggregationFilter(ComparisonType.GTE_AND_LT, Arrays.asList(1, 10));
        filter3 = new AggregationFilter(ComparisonType.GREATER_OR_EQUAL, Collections.singletonList(10));
        txn1 = new Bucket.Transaction(prodId, timeFilter, filter1, null, false);
        txn2 = new Bucket.Transaction(prodId, timeFilter, filter2, null, false);
        txn3 = new Bucket.Transaction(prodId, timeFilter, filter3, null, false);
        count1 = countTxnBkt(txn1, sqlUser);
        count2 = countTxnBkt(txn2, sqlUser);
        count3 = countTxnBkt(txn3, sqlUser);
        Assert.assertEquals(count1 + count2 + count3, count);
    }

    @DataProvider(name = "timefilterProvider")
    private Object[][] timefilterProvider() {
        return getTimeFilterDataProvider();
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAccountWithPriorOnly(String sqlUser, String queryContext) {
        MultiTenantContext.setTenant(tenant);
        String prodId = "6368494B622E0CB60F9C80FEB1D0F95F";
        String period = PeriodStrategy.Template.Month.name();

        // prior only to last month
        TimeFilter timeFilter = TimeFilter.priorOnly(1, period);
        Bucket.Transaction txn0 = new Bucket.Transaction(prodId, timeFilter, null, null, false);
        long count0 = countTxnBkt(txn0, sqlUser);

        // not within last month
        TimeFilter timeFilter1 = TimeFilter.within(1, period);
        Bucket.Transaction txn1 = new Bucket.Transaction(prodId, timeFilter1, null, null, true);
        Restriction restriction1 = getTxnRestriction(txn1);

        // not in current month
        TimeFilter timeFilter2 = TimeFilter.inCurrent(period);
        Bucket.Transaction txn2 = new Bucket.Transaction(prodId, timeFilter2, null, null, true);
        Restriction restriction2 = getTxnRestriction(txn2);

        // prior to last month
        TimeFilter timeFilter3 = TimeFilter.prior(1, period);
        Bucket.Transaction txn3 = new Bucket.Transaction(prodId, timeFilter3, null, null, false);
        Restriction restriction3 = getTxnRestriction(txn3);
        Restriction restriction = Restriction.builder().and(restriction1, restriction2, restriction3).build();

        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        long count = getEntityQueryService(sqlUser).getCount(frontEndQuery, DataCollection.Version.Blue, sqlUser);
        Assert.assertEquals(count0, count);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAccountContactWithTxn(String sqlUser, String queryContext) {
        String prodId = "o13brsbfF10fllM6VUZRxMO7wfo5I7Ks";
        // Check add up
        AggregationFilter filter = new AggregationFilter(ComparisonType.GT_AND_LT, Arrays.asList(0, 1000));
        Bucket.Transaction txn = new Bucket.Transaction(prodId, TimeFilter.ever(), filter, null, false);

        Restriction txnRestriction = getTxnRestriction(txn);
        Restriction accRestriction = Restriction.builder().let(BusinessEntity.Account, AccountAttr.CompanyName).gte("A")
                .build();
        Restriction cntRestriction = Restriction.builder().let(BusinessEntity.Contact, ContactAttr.Occupation)
                .eq("Analyst").build();

        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);

        FrontEndRestriction accountFERestriction = new FrontEndRestriction();
        accountFERestriction.setRestriction(Restriction.builder().and(txnRestriction, accRestriction).build());
        frontEndQuery.setAccountRestriction(accountFERestriction);

        FrontEndRestriction contactFERestriction = new FrontEndRestriction();
        contactFERestriction.setRestriction(cntRestriction);
        frontEndQuery.setContactRestriction(contactFERestriction);

        frontEndQuery.setMainEntity(BusinessEntity.Account);
        Long count = getEntityQueryService(sqlUser).getCount(frontEndQuery, DataCollection.Version.Blue, sqlUser);
        Assert.assertNotNull(count);
        testAndAssertCount(sqlUser, count, 1668);
    }

    private long countTxnBkt(Bucket.Transaction txn, String sqlUser) {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction restriction = getTxnRestriction(txn);
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        return getEntityQueryService(sqlUser).getCount(frontEndQuery, DataCollection.Version.Blue, sqlUser);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAccountDataWithTranslation(String sqlUser, String queryContext) {
        testAccountDataWithTranslation(true, sqlUser);
        // testAccountDataWithTranslation(false, sqlUser);
    }

    private void testAccountDataWithTranslation(boolean enforceTranslation, String sqlUser) {
        FrontEndQuery frontEndQuery = getAccountDataQuery();
        DataPage dataPage = getEntityQueryService(sqlUser).getData(frontEndQuery, DataCollection.Version.Blue, sqlUser,
                enforceTranslation);
        Assert.assertNotNull(dataPage);
        List<Map<String, Object>> data = dataPage.getData();
        Assert.assertFalse(data.isEmpty());
        data.forEach(row -> {
            Assert.assertTrue(row.containsKey(InterfaceName.AccountId.name()));
            Object techIndicatorVal = row.get("TechIndicator_AdobeTargetStandard");
            Assert.assertNotNull(techIndicatorVal);
            if (enforceTranslation) {
                String techIndicatorValue = (String) techIndicatorVal;
                Assert.assertTrue(techIndicatorValue.equals("Yes") || techIndicatorValue.equals("No"),
                        techIndicatorValue);
            } else {
                Long techIndicatorValue = (Long) techIndicatorVal;
                Assert.assertTrue(techIndicatorValue == 1 || techIndicatorValue == 2, techIndicatorValue.toString());
            }
        });
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAccountSql(String sqlUser, String queryContext) {
        FrontEndQuery frontEndQuery = getAccountDataQuery();
        String sql = getEntityQueryService(sqlUser).getQueryStr(frontEndQuery, DataCollection.Version.Blue, sqlUser,
                false);
        System.out.println(sql);
    }

    private FrontEndQuery getAccountDataQuery() {
        Restriction accRestriction = Restriction.builder()
                .let(BusinessEntity.Account, "TechIndicator_AdobeTargetStandard").isNotNull().build();
        FrontEndQuery frontEndQuery = new FrontEndQuery();

        FrontEndRestriction accountFERestriction = new FrontEndRestriction();
        accountFERestriction.setRestriction(accRestriction);
        frontEndQuery.setAccountRestriction(accountFERestriction);
        PageFilter pageFilter = new PageFilter(0, 10);
        frontEndQuery.setPageFilter(pageFilter);

        frontEndQuery.addLookups(BusinessEntity.Account, InterfaceName.AccountId.name(),
                "TechIndicator_AdobeTargetStandard");

        frontEndQuery.setMainEntity(BusinessEntity.Account);
        return frontEndQuery;
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAccountContactWithDeletedTxn(String sqlUser, String queryContext) {
        MultiTenantContext.setTenant(tenant);
        String prodId = "6368494B622E0CB60F9C80FEB1D0F95F";
        AggregationFilter filter = new AggregationFilter(ComparisonType.GT_AND_LT, Arrays.asList(0, 1000));
        Bucket.Transaction txn = new Bucket.Transaction(prodId, TimeFilter.ever(), filter, null, false);
        BucketRestriction txnRestriction = (BucketRestriction) getTxnRestriction(txn);
        txnRestriction.setIgnored(true);

        Restriction accRestriction = Restriction.builder().let(BusinessEntity.Account, AccountAttr.CompanyName).gte("A")
                .build();
        Restriction cntRestriction = Restriction.builder().let(BusinessEntity.Contact, ContactAttr.Occupation)
                .eq("Analyst").build();

        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);

        FrontEndRestriction accountFERestriction = new FrontEndRestriction();
        accountFERestriction.setRestriction(Restriction.builder().and(txnRestriction, accRestriction).build());
        frontEndQuery.setAccountRestriction(accountFERestriction);

        FrontEndRestriction contactFERestriction = new FrontEndRestriction();
        contactFERestriction.setRestriction(cntRestriction);
        frontEndQuery.setContactRestriction(contactFERestriction);

        frontEndQuery.setMainEntity(BusinessEntity.Account);
        Long count = getEntityQueryService(sqlUser).getCount(frontEndQuery, DataCollection.Version.Blue, sqlUser);
        Assert.assertNotNull(count);
        testAndAssertCount(sqlUser, count, 2019);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAccountEndsWith(String sqlUser, String queryContext) {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction1 = new FrontEndRestriction();
        Bucket bucket = Bucket.valueBkt(ComparisonType.ENDS_WITH, Collections.singletonList("Acufocus, Inc."));
        BucketRestriction endsWithRestriction = new BucketRestriction(BusinessEntity.Account,
                InterfaceName.LDC_Name.name(), bucket);
        frontEndRestriction1.setRestriction(endsWithRestriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction1);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        Long count = getEntityQueryService(sqlUser).getCount(frontEndQuery, DataCollection.Version.Blue, sqlUser);
        Assert.assertNotNull(count);
        testAndAssertCount(sqlUser, count, 1);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testContactAccountWithTxn(String sqlUser, String queryContext) {
        String prodId = "o13brsbfF10fllM6VUZRxMO7wfo5I7Ks";
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction1 = new FrontEndRestriction();
        Restriction restriction1 = Restriction.builder().let(BusinessEntity.Account, AccountAttr.CompanyName).gte("A")
                .build();
        Bucket.Transaction txn = new Bucket.Transaction(prodId, TimeFilter.ever(), null, null, false);
        Bucket bucket = Bucket.txnBkt(txn);
        Restriction txRestriction = new BucketRestriction(TRANSACTION_LOOKUP, bucket);
        Restriction logicalRestriction = Restriction.builder().and(restriction1, txRestriction).build();
        frontEndRestriction1.setRestriction(logicalRestriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction1);

        FrontEndRestriction frontEndRestriction2 = new FrontEndRestriction();
        Restriction restriction2 = Restriction.builder().let(BusinessEntity.Contact, ContactAttr.Occupation).gte("VP")
                .build();
        frontEndRestriction2.setRestriction(restriction2);
        frontEndQuery.setContactRestriction(frontEndRestriction2);

        frontEndQuery.setMainEntity(BusinessEntity.Contact);
        Long count = getEntityQueryService(sqlUser).getCount(frontEndQuery, DataCollection.Version.Blue, sqlUser);
        Assert.assertNotNull(count);
        testAndAssertCount(sqlUser, count, 168);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAccountContactCount(String sqlUser, String queryContext) {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction1 = new FrontEndRestriction();
        Restriction restriction1 = Restriction.builder().let(BusinessEntity.Account, AccountAttr.CompanyName).gte("A")
                .build();
        frontEndRestriction1.setRestriction(restriction1);
        frontEndQuery.setAccountRestriction(frontEndRestriction1);

        FrontEndRestriction frontEndRestriction2 = new FrontEndRestriction();
        Restriction restriction2 = Restriction.builder().let(BusinessEntity.Contact, ContactAttr.Occupation).gte("VP")
                .build();
        frontEndRestriction2.setRestriction(restriction2);
        frontEndQuery.setContactRestriction(frontEndRestriction2);

        frontEndQuery.setMainEntity(BusinessEntity.Account);
        long count = getEntityQueryService(sqlUser).getCount(frontEndQuery, DataCollection.Version.Blue, sqlUser);
        testAndAssertCount(sqlUser, count, 203);

        frontEndQuery.setMainEntity(BusinessEntity.Contact);
        count = getEntityQueryService(sqlUser).getCount(frontEndQuery, DataCollection.Version.Blue, sqlUser);
        testAndAssertCount(sqlUser, count, 217);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testContactDataWithAccountIds(String sqlUser, String queryContext) {
        // mimic a segment get from metadata api
        MetadataSegment segment = new MetadataSegment();
        Restriction accountRestrictionInternal = Restriction.builder()
                .let(BusinessEntity.Account, AccountAttr.CompanyName).gte("A").build();
        segment.setAccountRestriction(accountRestrictionInternal);
        Restriction contactRestrictionInternal = Restriction.builder()
                .let(BusinessEntity.Contact, ContactAttr.Occupation).gte("VP").build();
        segment.setContactRestriction(contactRestrictionInternal);

        // front end query from segment restrictions
        FrontEndQuery queryFromSegment = FrontEndQuery.fromSegment(segment);
        queryFromSegment.setEvaluationDateStr(maxTransactionDate);

        // get account ids first
        queryFromSegment.setMainEntity(BusinessEntity.Account);
        queryFromSegment.setLookups( //
                Collections.singletonList(new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name())));
        queryFromSegment.setSort(new FrontEndSort(
                Collections.singletonList(new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name())),
                false));
        queryFromSegment.setPageFilter(new PageFilter(0, 10));
        DataPage dataPage = getEntityQueryService(sqlUser).getData(queryFromSegment, DataCollection.Version.Blue,
                sqlUser, false);
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
        contactQuery.setEvaluationDateStr(maxTransactionDate);
        contactQuery.setMainEntity(BusinessEntity.Contact);
        contactQuery.setContactRestriction(frontEndRestriction);
        contactQuery.setLookups(Arrays.asList( //
                new AttributeLookup(BusinessEntity.Contact, InterfaceName.AccountId.name()),
                new AttributeLookup(BusinessEntity.Contact, InterfaceName.ContactId.name())));
        contactQuery.setPageFilter(new PageFilter(0, 100));
        dataPage = getEntityQueryService(sqlUser).getData(contactQuery, DataCollection.Version.Blue, sqlUser, false);
        Assert.assertEquals(dataPage.getData().size(), 10);
        for (Map<String, Object> contact : dataPage.getData()) {
            Object accountId = contact.get(InterfaceName.AccountId.name());
            Assert.assertTrue(accountIds.contains(accountId));
        }
        FrontEndQuery emptyContactQuery = new FrontEndQuery();
        emptyContactQuery.setMainEntity(BusinessEntity.Contact);
        emptyContactQuery.setContactRestriction(frontEndRestriction);
        emptyContactQuery.setLookups(
                Collections.singletonList(new AttributeLookup(BusinessEntity.Contact, InterfaceName.Email.name())));
        dataPage = getEntityQueryService(sqlUser).getData(emptyContactQuery, DataCollection.Version.Blue, sqlUser,
                false);
        Assert.assertEquals(dataPage.getData().size(), 10);
        for (Map<String, Object> contact : dataPage.getData()) {
            Assert.assertTrue(contact.containsKey(InterfaceName.Email.toString()));
        }
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testLogicalOr(String sqlUser, String queryContext) {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);

        Restriction restriction1 = Restriction.builder().let(BusinessEntity.Account, AccountAttr.CompanyName)
                .contains("On").build();
        Restriction restriction2 = Restriction.builder().let(BusinessEntity.Account, AccountAttr.CompanyName)
                .contains("Ca").build();
        Restriction or = Restriction.builder().or(restriction1, restriction2).build();

        // res 1
        frontEndRestriction.setRestriction(restriction1);
        long count1 = getEntityQueryService(sqlUser).getCount(frontEndQuery, DataCollection.Version.Blue, sqlUser);
        Assert.assertTrue(count1 > 0, String.valueOf(count1));

        // res 2
        frontEndRestriction.setRestriction(restriction2);
        long count2 = getEntityQueryService(sqlUser).getCount(frontEndQuery, DataCollection.Version.Blue, sqlUser);
        Assert.assertTrue(count2 > 0, String.valueOf(count2));

        // or
        frontEndRestriction.setRestriction(or);
        long count = getEntityQueryService(sqlUser).getCount(frontEndQuery, DataCollection.Version.Blue, sqlUser);
        Assert.assertTrue(count >= count1, String.format("%d >= %d", count, count1));
        Assert.assertTrue(count >= count2, String.format("%d >= %d", count, count2));
        Assert.assertTrue(count <= count1 + count2, String.format("%d <= %d + %d", count, count1, count2));
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testScoreData(String sqlUser, String queryContext) {
        String engineId = "engine_kzuu2r54skmqxpw1nr2lpw";
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction restriction = Restriction.builder().let(BusinessEntity.Account, AccountAttr.CompanyName).isNotNull()
                .build();
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        Bucket contactBkt = Bucket.valueBkt(ComparisonType.CONTAINS, Collections.singletonList("Manager"));
        Restriction contactRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Contact, ContactAttr.Occupation), contactBkt);
        frontEndQuery.setContactRestriction(new FrontEndRestriction(contactRestriction));
        frontEndQuery.setPageFilter(new PageFilter(0, 10));
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setSort(new FrontEndSort(
                Collections.singletonList(new AttributeLookup(BusinessEntity.Account, AccountAttr.CompanyName)),
                false));

        String ratingField = RatingEngine.toRatingAttrName(engineId, RatingEngine.ScoreType.Rating);
        String scoreField = RatingEngine.toRatingAttrName(engineId, RatingEngine.ScoreType.Score);
        frontEndQuery.setLookups(Arrays.asList(//
                new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name()),
                new AttributeLookup(BusinessEntity.Rating, ratingField),
                new AttributeLookup(BusinessEntity.Rating, scoreField)));

        DataPage dataPage = getEntityQueryService(sqlUser).getData(frontEndQuery, DataCollection.Version.Blue, sqlUser,
                false);
        Assert.assertNotNull(dataPage);
        List<Map<String, Object>> data = dataPage.getData();
        Assert.assertFalse(data.isEmpty());
        data.forEach(row -> {
            Assert.assertTrue(row.containsKey(ratingField));
            Assert.assertTrue(row.containsKey(scoreField));
        });

        // only get scores for A, B
        Restriction selectedScores = Restriction.builder() //
                .let(BusinessEntity.Rating, ratingField) //
                .inCollection(Arrays.asList( //
                        RatingBucketName.A.getName(), RatingBucketName.B.getName())) //
                .build();
        Restriction restriction2 = Restriction.builder().and(restriction, selectedScores).build();
        frontEndRestriction.setRestriction(restriction2);
        frontEndQuery.setAccountRestriction(frontEndRestriction);

        dataPage = getEntityQueryService(sqlUser).getData(frontEndQuery, DataCollection.Version.Blue, sqlUser, false);
        Assert.assertNotNull(dataPage);
        data = dataPage.getData();
        Assert.assertFalse(data.isEmpty());
        data.forEach(row -> {
            Assert.assertTrue(row.containsKey(ratingField));
            String score = (String) row.get(ratingField);
            Assert.assertTrue( //
                    RatingBucketName.A.getName().equals(score) || RatingBucketName.B.getName().equals(score), score);
        });
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testScoreCount(String sqlUser, String queryContext) {
        // String sqlUser = SEGMENT_USER;
        String engineId = "engine_kzuu2r54skmqxpw1nr2lpw";
        RatingEngineFrontEndQuery frontEndQuery = new RatingEngineFrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction accountRestriction = Restriction.builder().let(BusinessEntity.Account, AccountAttr.CompanyName)
                .gte("A").build();
        frontEndRestriction.setRestriction(accountRestriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);

        Bucket contactBkt = Bucket.valueBkt(ComparisonType.CONTAINS, Collections.singletonList("Analyst"));
        Restriction contactRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Contact, ContactAttr.Occupation), contactBkt);
        frontEndQuery.setContactRestriction(new FrontEndRestriction(contactRestriction));

        frontEndQuery.setRatingEngineId(engineId);
        frontEndQuery.setMainEntity(BusinessEntity.Account);

        Map<String, Long> ratingCounts = getEntityQueryService(sqlUser).getRatingCount(frontEndQuery,
                DataCollection.Version.Blue, sqlUser);
        Assert.assertNotNull(ratingCounts);
        Assert.assertFalse(ratingCounts.isEmpty());
        ratingCounts.forEach((score, count) -> {
            // System.out.println(score + ":" + count);
            if (RatingBucketName.A.getName().equals(score)) {
                Assert.assertEquals(count, Long.valueOf(78));
            } else if (RatingBucketName.B.getName().equals(score)) {
                Assert.assertEquals(count, Long.valueOf(136));
            } else if (RatingBucketName.C.getName().equals(score)) {
                Assert.assertEquals(count, Long.valueOf(508));
            } else if (RatingBucketName.D.getName().equals(score)) {
                Assert.assertEquals(count, Long.valueOf(977));
            } else {
                Assert.fail("Unexpected score: " + score);
            }
        });
    }

}
