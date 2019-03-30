package com.latticeengines.objectapi.service.impl;

import static com.latticeengines.domain.exposed.util.RestrictionUtils.TRANSACTION_LOOKUP;
import static org.mockito.ArgumentMatchers.any;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.mockito.Mockito;
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
import com.latticeengines.objectapi.service.EventQueryService;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;

public class EntityQueryServiceImplTestNG extends QueryServiceImplTestNGBase {

    private static final String ATTR_ACCOUNT_NAME = "CompanyName";
    private static final String ATTR_CONTACT_TITLE = "Occupation";

    @Inject
    private EntityQueryServiceImpl entityQueryService;

    @Inject
    private EventQueryService eventQueryService;

    @BeforeClass(groups = { "functional", "manual" })
    public void setup() {
        super.setup("2");
    }

    @Test(groups = "manual")
    public void testMax() {
        DataCollectionProxy proxy = Mockito.mock(DataCollectionProxy.class);
        Mockito.when(proxy.getAttrRepo(any(), any())).thenReturn(attrRepo);
        Mockito.when(proxy.getTableName(any(), any(), any()))
                .thenReturn("qa_juan_dataattribute_test_0212_02_account_2019_02_26_06_59_15_utc");
        queryEvaluatorService.setDataCollectionProxy(proxy);

        Set<AttributeLookup> set = new HashSet<>();
        AttributeLookup accout_1 = new AttributeLookup(BusinessEntity.Account, "user_createddate");
        AttributeLookup accout_2 = new AttributeLookup(BusinessEntity.Account, "user_testdate_dd_mmm_yyyy__8h");
        AttributeLookup accout_3 = new AttributeLookup(BusinessEntity.Account,
                "user_testdate_column_dd_mmm_yyyy_withoutdate");
        AttributeLookup contact_1 = new AttributeLookup(BusinessEntity.Contact,
                "user_testdate_column_dd_mmm_yyyy_withoutdate");
        AttributeLookup contact_2 = new AttributeLookup(BusinessEntity.Contact,
                "user_created_date_mm_dd_yyyy_hh_mm_ss_12h");
        // set.addAll(Arrays.asList(accout_2, accout_3, contact_1, contact_2));
        set.addAll(Arrays.asList(accout_2, accout_3));
        // set.addAll(Arrays.asList(contact_1, contact_2));
        Map<AttributeLookup, Object> results = entityQueryService.getMaxDates(set, DataCollection.Version.Blue);
        results.forEach((k, v) -> {
            System.out.println(k + ": " + v);
        });
    }

    @Test(groups = "manual")
    public void testMaxViaFrontEndQuery() {
        DataCollectionProxy proxy = Mockito.mock(DataCollectionProxy.class);
        Mockito.when(proxy.getAttrRepo(any(), any())).thenReturn(attrRepo);
        Mockito.when(proxy.getTableName(any(), any(), any()))
                .thenReturn("qa_juan_dataattribute_test_0212_02_account_2019_02_26_06_59_15_utc");
        queryEvaluatorService.setDataCollectionProxy(proxy);

        Set<AttributeLookup> set = new HashSet<>();
        AttributeLookup accout_1 = new AttributeLookup(BusinessEntity.Account, "user_createddate");
        AttributeLookup accout_2 = new AttributeLookup(BusinessEntity.Account, "user_testdate_dd_mmm_yyyy__8h");
        AttributeLookup accout_3 = new AttributeLookup(BusinessEntity.Account,
                "user_testdate_column_dd_mmm_yyyy_withoutdate");
        AttributeLookup contact_1 = new AttributeLookup(BusinessEntity.Contact,
                "user_testdate_column_dd_mmm_yyyy_withoutdate");
        AttributeLookup contact_2 = new AttributeLookup(BusinessEntity.Contact,
                "user_created_date_mm_dd_yyyy_hh_mm_ss_12h");
        // set.addAll(Arrays.asList(accout_2, accout_3, contact_1, contact_2));
        set.addAll(Arrays.asList(accout_2, accout_3));
        // set.addAll(Arrays.asList(contact_1, contact_2));
        Map<AttributeLookup, Object> results = entityQueryService.getMaxDatesViaFrontEndQuery(set,
                DataCollection.Version.Blue);
        results.forEach((k, v) -> {
            System.out.println(k + ": " + v);
        });
    }

    @Test(groups = "functional")
    public void testDistinctCount() {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        frontEndQuery.addLookups(BusinessEntity.Product, InterfaceName.ProductId.name());
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Product);
        frontEndQuery.setDistinct(true);

        Long count = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertNotNull(count);
        Assert.assertEquals(count, Long.valueOf(1));
    }

    @Test(groups = "functional")
    public void testProductCountAndData() {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction restriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("A").build();
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Product);

        Long count = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertNotNull(count);
        Assert.assertEquals(count, Long.valueOf(10));

        DataPage dataPage = entityQueryService.getData(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER, false);
        for (Map<String, Object> product : dataPage.getData()) {
            Assert.assertTrue(product.containsKey(InterfaceName.ProductId.name()));
            Assert.assertTrue(product.containsKey(InterfaceName.ProductName.name()));
        }
    }

    @Test(groups = "functional")
    public void testAccountCount() {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction restriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("A").build();
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        Long count = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertNotNull(count);
        Assert.assertEquals(count, Long.valueOf(33393));
    }

    @Test(groups = "functional")
    public void testContactCount() {
        FrontEndQuery frontEndQuery = JsonUtils.deserialize(
                "{\n" + "  \"main_entity\": \"Contact\",\n" + "  \"distinct\": false\n" + "}", FrontEndQuery.class);
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        frontEndQuery.setMainEntity(BusinessEntity.Contact);
        long count = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertEquals(count, 132628);
    }

    @Test(groups = "functional")
    public void testMetricRestriction() {
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
        frontEndQuery.addLookups(BusinessEntity.ProductHierarchy, phAttr);
        long count = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertEquals(count, 33680);

        frontEndQuery.setPageFilter(new PageFilter(0, 10));
        DataPage dataPage = entityQueryService.getData(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER, false);
        Assert.assertNotNull(dataPage);
        Assert.assertTrue(CollectionUtils.isNotEmpty(dataPage.getData()));
        // not support querying PH attrs
        Assert.assertFalse(dataPage.getData().get(0).containsKey(phAttr));
    }

    @Test(groups = "functional", dataProvider = "timefilterProvider")
    public void testAccountWithTxn(TimeFilter timeFilter, long expectedTotal) {
        MultiTenantContext.setTenant(tenant);
        String prodId = "o13brsbfF10fllM6VUZRxMO7wfo5I7Ks";

        // Ever Purchased
        Bucket.Transaction txn = new Bucket.Transaction(prodId, timeFilter, null, null, false);
        long totalCount = countTxnBkt(txn);
        Assert.assertEquals(totalCount, expectedTotal);

        // Ever, Amount > 0, Quantity > 0
        AggregationFilter greaterThan0 = new AggregationFilter(ComparisonType.GREATER_THAN,
                Collections.singletonList(0));
        txn = new Bucket.Transaction(prodId, timeFilter, greaterThan0, greaterThan0, false);
        long count = countTxnBkt(txn);
        Assert.assertTrue(count <= totalCount, String.format("%d <= %d", count, totalCount));

        // Check add up
        AggregationFilter filter1 = new AggregationFilter(ComparisonType.GT_AND_LT, Arrays.asList(0, 1000));
        AggregationFilter filter2 = new AggregationFilter(ComparisonType.GTE_AND_LT, Arrays.asList(1000, 3000));
        AggregationFilter filter3 = new AggregationFilter(ComparisonType.GREATER_OR_EQUAL,
                Collections.singletonList(3000));
        Bucket.Transaction txn1 = new Bucket.Transaction(prodId, timeFilter, filter1, null, false);
        Bucket.Transaction txn2 = new Bucket.Transaction(prodId, timeFilter, filter2, null, false);
        Bucket.Transaction txn3 = new Bucket.Transaction(prodId, timeFilter, filter3, null, false);
        long count1 = countTxnBkt(txn1);
        long count2 = countTxnBkt(txn2);
        long count3 = countTxnBkt(txn3);
        Assert.assertEquals(count1 + count2 + count3, count);

        filter1 = new AggregationFilter(ComparisonType.GT_AND_LT, Arrays.asList(0, 1));
        filter2 = new AggregationFilter(ComparisonType.GTE_AND_LT, Arrays.asList(1, 10));
        filter3 = new AggregationFilter(ComparisonType.GREATER_OR_EQUAL, Collections.singletonList(10));
        txn1 = new Bucket.Transaction(prodId, timeFilter, filter1, null, false);
        txn2 = new Bucket.Transaction(prodId, timeFilter, filter2, null, false);
        txn3 = new Bucket.Transaction(prodId, timeFilter, filter3, null, false);
        count1 = countTxnBkt(txn1);
        count2 = countTxnBkt(txn2);
        count3 = countTxnBkt(txn3);
        Assert.assertEquals(count1 + count2 + count3, count);
    }

    @DataProvider(name = "timefilterProvider", parallel = true)
    public Object[][] timefilterProvider() {
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
                { TimeFilter.ever(), 24636L }, //
                { currentMonth, 1983L }, //
                { lastMonth, 2056L }, //
                { betweendates, 2185L }, //
        };
    }

    @Test(groups = "functional")
    public void testAccountWithPriorOnly() {
        MultiTenantContext.setTenant(tenant);
        String prodId = "6368494B622E0CB60F9C80FEB1D0F95F";
        String period = PeriodStrategy.Template.Month.name();

        // prior only to last month
        TimeFilter timeFilter = TimeFilter.priorOnly(1, period);
        Bucket.Transaction txn0 = new Bucket.Transaction(prodId, timeFilter, null, null, false);
        long count0 = countTxnBkt(txn0);

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
        long count = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertEquals(count0, count);
    }

    @Test(groups = "functional")
    public void testAccountContactWithTxn() {
        String prodId = "o13brsbfF10fllM6VUZRxMO7wfo5I7Ks";
        // Check add up
        AggregationFilter filter = new AggregationFilter(ComparisonType.GT_AND_LT, Arrays.asList(0, 1000));
        Bucket.Transaction txn = new Bucket.Transaction(prodId, TimeFilter.ever(), filter, null, false);

        Restriction txnRestriction = getTxnRestriction(txn);
        Restriction accRestriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("A")
                .build();
        Restriction cntRestriction = Restriction.builder().let(BusinessEntity.Contact, ATTR_CONTACT_TITLE).eq("Analyst")
                .build();

        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);

        FrontEndRestriction accountFERestriction = new FrontEndRestriction();
        accountFERestriction.setRestriction(Restriction.builder().and(txnRestriction, accRestriction).build());
        frontEndQuery.setAccountRestriction(accountFERestriction);

        FrontEndRestriction contactFERestriction = new FrontEndRestriction();
        contactFERestriction.setRestriction(cntRestriction);
        frontEndQuery.setContactRestriction(contactFERestriction);

        frontEndQuery.setMainEntity(BusinessEntity.Account);
        Long count = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertNotNull(count);
        Assert.assertEquals(count, Long.valueOf(1668));
    }

    @Test(groups = "functional")
    public void testAccountDataWithTranslation() {
        testAccountDataWithTranslation(true);
        testAccountDataWithTranslation(false);
    }

    private void testAccountDataWithTranslation(boolean enforceTranslation) {
        FrontEndQuery frontEndQuery = getAccountDataQuery();
        DataPage dataPage = entityQueryService.getData(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER,
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

    @Test(groups = "functional")
    public void testAccountSql() {
        FrontEndQuery frontEndQuery = getAccountDataQuery();
        String sql = entityQueryService.getQueryStr(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
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

    @Test(groups = "functional")
    public void testAccountContactWithDeletedTxn() {
        MultiTenantContext.setTenant(tenant);
        String prodId = "6368494B622E0CB60F9C80FEB1D0F95F";
        AggregationFilter filter = new AggregationFilter(ComparisonType.GT_AND_LT, Arrays.asList(0, 1000));
        Bucket.Transaction txn = new Bucket.Transaction(prodId, TimeFilter.ever(), filter, null, false);
        BucketRestriction txnRestriction = (BucketRestriction) getTxnRestriction(txn);
        txnRestriction.setIgnored(true);

        Restriction accRestriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("A")
                .build();
        Restriction cntRestriction = Restriction.builder().let(BusinessEntity.Contact, ATTR_CONTACT_TITLE).eq("Analyst")
                .build();

        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);

        FrontEndRestriction accountFERestriction = new FrontEndRestriction();
        accountFERestriction.setRestriction(Restriction.builder().and(txnRestriction, accRestriction).build());
        frontEndQuery.setAccountRestriction(accountFERestriction);

        FrontEndRestriction contactFERestriction = new FrontEndRestriction();
        contactFERestriction.setRestriction(cntRestriction);
        frontEndQuery.setContactRestriction(contactFERestriction);

        frontEndQuery.setMainEntity(BusinessEntity.Account);
        Long count = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertNotNull(count);
        Assert.assertEquals(count, Long.valueOf(2019));
    }

    private long countTxnBkt(Bucket.Transaction txn) {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction restriction = getTxnRestriction(txn);
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        return entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
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

    @Test(groups = "functional")
    public void testAccountEndsWith() {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction1 = new FrontEndRestriction();
        Bucket bucket = Bucket.valueBkt(ComparisonType.ENDS_WITH, Collections.singletonList("Acufocus, Inc."));
        BucketRestriction endsWithRestriction = new BucketRestriction(BusinessEntity.Account,
                InterfaceName.LDC_Name.name(), bucket);
        frontEndRestriction1.setRestriction(endsWithRestriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction1);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        Long count = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertNotNull(count);
        Assert.assertEquals(count, Long.valueOf(1));
    }

    @Test(groups = "functional")
    public void testContactAccountWithTxn() {
        String prodId = "o13brsbfF10fllM6VUZRxMO7wfo5I7Ks";
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction1 = new FrontEndRestriction();
        Restriction restriction1 = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("A")
                .build();
        Bucket.Transaction txn = new Bucket.Transaction(prodId, TimeFilter.ever(), null, null, false);
        Bucket bucket = Bucket.txnBkt(txn);
        Restriction txRestriction = new BucketRestriction(TRANSACTION_LOOKUP, bucket);
        Restriction logicalRestriction = Restriction.builder().and(restriction1, txRestriction).build();
        frontEndRestriction1.setRestriction(logicalRestriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction1);

        FrontEndRestriction frontEndRestriction2 = new FrontEndRestriction();
        Restriction restriction2 = Restriction.builder().let(BusinessEntity.Contact, ATTR_CONTACT_TITLE).gte("VP")
                .build();
        frontEndRestriction2.setRestriction(restriction2);
        frontEndQuery.setContactRestriction(frontEndRestriction2);

        frontEndQuery.setMainEntity(BusinessEntity.Contact);
        Long count = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertNotNull(count);
        Assert.assertEquals(count, Long.valueOf(168));

    }

    @Test(groups = "functional")
    public void testAccountContactCount() {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
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
        Long count = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertNotNull(count);
        Assert.assertEquals(count, Long.valueOf(203L));

        frontEndQuery.setMainEntity(BusinessEntity.Contact);
        count = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertNotNull(count);
        Assert.assertEquals(count, Long.valueOf(217L));
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
        queryFromSegment.setEvaluationDateStr(maxTransactionDate);

        // get account ids first
        queryFromSegment.setMainEntity(BusinessEntity.Account);
        queryFromSegment.setLookups( //
                Collections.singletonList(new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name())));
        queryFromSegment.setSort(new FrontEndSort(
                Collections.singletonList(new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name())),
                false));
        queryFromSegment.setPageFilter(new PageFilter(0, 10));
        DataPage dataPage = entityQueryService.getData(queryFromSegment, DataCollection.Version.Blue, SEGMENT_USER,
                false);
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
        dataPage = entityQueryService.getData(contactQuery, DataCollection.Version.Blue, SEGMENT_USER, false);
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
        dataPage = entityQueryService.getData(emptyContactQuery, DataCollection.Version.Blue, SEGMENT_USER, false);
        Assert.assertEquals(dataPage.getData().size(), 10);
        for (Map<String, Object> contact : dataPage.getData()) {
            Assert.assertTrue(contact.containsKey(InterfaceName.Email.toString()));
        }
    }

    @Test(groups = "functional")
    public void testLogicalOr() {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);

        Restriction restriction1 = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).contains("On")
                .build();
        Restriction restriction2 = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).contains("Ca")
                .build();
        Restriction or = Restriction.builder().or(restriction1, restriction2).build();

        // res 1
        frontEndRestriction.setRestriction(restriction1);
        long count1 = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertTrue(count1 > 0, String.valueOf(count1));

        // res 2
        frontEndRestriction.setRestriction(restriction2);
        long count2 = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertTrue(count2 > 0, String.valueOf(count2));

        // or
        frontEndRestriction.setRestriction(or);
        long count = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertTrue(count >= count1, String.format("%d >= %d", count, count1));
        Assert.assertTrue(count >= count2, String.format("%d >= %d", count, count2));
        Assert.assertTrue(count <= count1 + count2, String.format("%d <= %d + %d", count, count1, count2));
    }

    @Test(groups = "functional")
    public void testScoreData() {
        String engineId = "engine_kzuu2r54skmqxpw1nr2lpw";
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction restriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).isNotNull()
                .build();
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        Bucket contactBkt = Bucket.valueBkt(ComparisonType.CONTAINS, Collections.singletonList("Manager"));
        Restriction contactRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Contact, ATTR_CONTACT_TITLE), contactBkt);
        frontEndQuery.setContactRestriction(new FrontEndRestriction(contactRestriction));
        frontEndQuery.setPageFilter(new PageFilter(0, 10));
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setSort(new FrontEndSort(
                Collections.singletonList(new AttributeLookup(BusinessEntity.Account, ATTR_ACCOUNT_NAME)), false));

        String ratingField = RatingEngine.toRatingAttrName(engineId, RatingEngine.ScoreType.Rating);
        String scoreField = RatingEngine.toRatingAttrName(engineId, RatingEngine.ScoreType.Score);
        String evField = RatingEngine.toRatingAttrName(engineId, RatingEngine.ScoreType.ExpectedRevenue);
        frontEndQuery.setLookups(Arrays.asList(//
                new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name()),
                new AttributeLookup(BusinessEntity.Rating, ratingField),
                new AttributeLookup(BusinessEntity.Rating, scoreField)));

        DataPage dataPage = entityQueryService.getData(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER, false);
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

        dataPage = entityQueryService.getData(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER, false);
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

    @Test(groups = "functional")
    public void testScoreCount() {
        String engineId = "engine_kzuu2r54skmqxpw1nr2lpw";
        RatingEngineFrontEndQuery frontEndQuery = new RatingEngineFrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction accountRestriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("A")
                .build();
        frontEndRestriction.setRestriction(accountRestriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);

        Bucket contactBkt = Bucket.valueBkt(ComparisonType.CONTAINS, Collections.singletonList("Analyst"));
        Restriction contactRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Contact, ATTR_CONTACT_TITLE), contactBkt);
        frontEndQuery.setContactRestriction(new FrontEndRestriction(contactRestriction));

        frontEndQuery.setRatingEngineId(engineId);
        frontEndQuery.setMainEntity(BusinessEntity.Account);

        Map<String, Long> ratingCounts = entityQueryService.getRatingCount(frontEndQuery, DataCollection.Version.Blue,
                SEGMENT_USER);
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
