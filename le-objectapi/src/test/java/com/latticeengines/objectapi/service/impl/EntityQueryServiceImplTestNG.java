package com.latticeengines.objectapi.service.impl;

import static com.latticeengines.domain.exposed.util.RestrictionUtils.TRANSACTION_LOOKUP;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.latticeengines.common.exposed.util.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

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

    private static final String ATTR_ACCOUNT_NAME = "name";
    private static final String ATTR_CONTACT_TITLE = "Title";

    @Inject
    private EntityQueryService entityQueryService;

    @Inject
    private EventQueryService eventQueryService;

    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
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
        Assert.assertEquals(count, new Long(1));
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
        Assert.assertEquals(count, new Long(149L));

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
        Assert.assertEquals(count, new Long(3165L));
    }

    @Test(groups = "functional")
    public void testContactCount() {
        FrontEndQuery frontEndQuery = JsonUtils.deserialize("{\n" +
                "  \"main_entity\": \"Contact\",\n" +
                "  \"distinct\": false\n" +
                "}", FrontEndQuery.class);
//        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
//        frontEndQuery.setMainEntity(BusinessEntity.Contact);
        DataPage dataPage = entityQueryService.getData(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER, true);
//        Long count = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
//        Assert.assertNotNull(count);
//        Assert.assertEquals(count, new Long(3165L));
    }

    @Test(groups = "functional", enabled = false)
    public void testMetricRestriction() {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Bucket bucket = Bucket.chgBkt(Bucket.Change.Direction.DEC, Bucket.Change.ComparisonType.AS_MUCH_AS,
                Collections.singletonList(5));
        Restriction restriction = new BucketRestriction(BusinessEntity.PurchaseHistory,
                "AM_drZvmxtPAib4xI6tWtQobEvi6B9BeTQ3__M_1__M_2_3__SC", bucket);
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        Long count = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertNotNull(count);
        Assert.assertEquals(count, Long.valueOf(716L));
    }

    @Test(groups = "functional", dataProvider = "timefilterProvider")
    public void testAccountWithTxn(TimeFilter timeFilter, long expectedTotal) {
        MultiTenantContext.setTenant(tenant);
        String prodId = "6368494B622E0CB60F9C80FEB1D0F95F";

        // Ever Purchased
        Bucket.Transaction txn = new Bucket.Transaction(prodId, timeFilter, null, null, false);
        long totalCount = countTxnBkt(txn);
        Assert.assertEquals(totalCount, expectedTotal);

        // Ever, Amount > 0, Quantity > 0
        AggregationFilter greaterThan0 = new AggregationFilter(ComparisonType.GREATER_THAN,
                Collections.singletonList(0));
        txn = new Bucket.Transaction(prodId, timeFilter, greaterThan0, greaterThan0, false);
        long count = countTxnBkt(txn);
        Assert.assertEquals(count, totalCount);

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
                { TimeFilter.ever(), 832L }, //
                { currentMonth, 223L }, //
                { lastMonth, 218L }, //
                { betweendates, 223L }, //
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
        String prodId = "6368494B622E0CB60F9C80FEB1D0F95F";
        // Check add up
        AggregationFilter filter = new AggregationFilter(ComparisonType.GT_AND_LT, Arrays.asList(0, 1000));
        Bucket.Transaction txn = new Bucket.Transaction(prodId, TimeFilter.ever(), filter, null, false);

        Restriction txnRestriction = getTxnRestriction(txn);
        Restriction accRestriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("A")
                .build();
        Restriction cntRestriction = Restriction.builder().let(BusinessEntity.Contact, InterfaceName.Title.name())
                .eq("Buyer").build();

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
        Assert.assertEquals(count, new Long(43));
    }

    @Test(groups = "functional")
    public void testAccountDataWithTranslation() {
        testAccountDataWithTranslation(true);
        testAccountDataWithTranslation(false);
    }

    private void testAccountDataWithTranslation(boolean enforceTranslation) {
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
        DataPage dataPage = entityQueryService.getData(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER, enforceTranslation);
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
    public void testAccountContactWithDeletedTxn() {
        MultiTenantContext.setTenant(tenant);
        String prodId = "6368494B622E0CB60F9C80FEB1D0F95F";
        AggregationFilter filter = new AggregationFilter(ComparisonType.GT_AND_LT, Arrays.asList(0, 1000));
        Bucket.Transaction txn = new Bucket.Transaction(prodId, TimeFilter.ever(), filter, null, false);
        BucketRestriction txnRestriction = (BucketRestriction) getTxnRestriction(txn);
        txnRestriction.setIgnored(true);

        Restriction accRestriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("A")
                .build();
        Restriction cntRestriction = Restriction.builder().let(BusinessEntity.Contact, InterfaceName.Title.name())
                .eq("Buyer").build();

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
        Assert.assertEquals(count, new Long(287));
    }

    private long countTxnBkt(Bucket.Transaction txn) {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction restriction = getTxnRestriction(txn);
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        Long count = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertNotNull(count);
        return count;
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
        frontEndQuery.setTargetProductIds(Collections.singletonList("6368494B622E0CB60F9C80FEB1D0F95F"));
        Long count = eventQueryService.getScoringCount(frontEndQuery, DataCollection.Version.Blue);
        Assert.assertNotNull(count);
        return count;
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
        Assert.assertEquals(count, new Long(1L));
    }

    @Test(groups = "functional")
    public void testContactAccountWithTxn() {
        String prodId = "6368494B622E0CB60F9C80FEB1D0F95F";
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
        Assert.assertEquals(count, new Long(15L));

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
        Assert.assertEquals(count, new Long(43L));

        frontEndQuery.setMainEntity(BusinessEntity.Contact);
        count = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
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
        Restriction restriction1 = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).contains("On")
                .build();
        Restriction restriction2 = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).contains("Ca")
                .build();
        frontEndRestriction.setRestriction(Restriction.builder().or(restriction1, restriction2).build());
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        long count = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertEquals(count, 1226L);
    }

    @Test(groups = "functional")
    public void testScoreData() {
        String engineId = "engine_auoinzzzt5k4s3d_dxirvg";
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
                new AttributeLookup(BusinessEntity.Rating, scoreField),
                new AttributeLookup(BusinessEntity.Rating, evField)));

        DataPage dataPage = entityQueryService.getData(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER, false);
        Assert.assertNotNull(dataPage);
        List<Map<String, Object>> data = dataPage.getData();
        Assert.assertFalse(data.isEmpty());
        data.forEach(row -> {
            Assert.assertTrue(row.containsKey(ratingField));
            Assert.assertTrue(row.containsKey(scoreField));
            Assert.assertTrue(row.containsKey(evField));
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
            Assert.assertEquals(score, RatingBucketName.B.getName());
        });
    }

    @Test(groups = "functional")
    public void testScoreCount() {
        String engineId = "engine_auoinzzzt5k4s3d_dxirvg";
        RatingEngineFrontEndQuery frontEndQuery = new RatingEngineFrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction accountRestriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("A")
                .build();
        frontEndRestriction.setRestriction(accountRestriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);

        Bucket contactBkt = Bucket.valueBkt(ComparisonType.CONTAINS, Collections.singletonList("Manager"));
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
            System.out.println(score + ":" + count);
            if (RatingBucketName.B.getName().equals(score)) {
                Assert.assertEquals((long) count, 2L);
            } else if (RatingBucketName.C.getName().equals(score)) {
                Assert.assertEquals((long) count, 4L);
            } else if (RatingBucketName.D.getName().equals(score)) {
                Assert.assertEquals((long) count, 4L);
            }
        });
    }

}
