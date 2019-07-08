package com.latticeengines.objectapi.service.impl;

import static com.latticeengines.domain.exposed.util.RestrictionUtils.TRANSACTION_LOOKUP;

import java.util.Arrays;
import java.util.Collections;

import javax.inject.Inject;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.util.TimeFilterTranslator;

public class EntityQueryServiceImplNegativeTxnTestNG extends QueryServiceImplTestNGBase {

    @Inject
    private EntityQueryServiceImpl entityQueryService;

    private final String prodId = "104061194E2216B01B3C6D4817DBCEA7";

    private static final Logger log = LoggerFactory.getLogger(EntityQueryServiceImplNegativeTxnTestNG.class);

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestData(5);
    }

    @Test(groups = "functional", dataProvider = "timefilterProvider")
    public void testAccountWithTxn(String sqlUser, TimeFilter timeFilter, long expectedTotal) {
        MultiTenantContext.setTenant(tenant);
        TimeFilterTranslator timeTranslator = entityQueryService.getTransactionService()
                .getTimeFilterTranslator(maxTransactionDate);
        Pair<String, String> range = timeTranslator.translateRange(timeFilter);

        // 1. check positive case
        Restriction restriction = Restriction.builder() //
                .and(Restriction.builder().let(BusinessEntity.Transaction, "ProductId").eq(prodId).build(),
                        range == null ? null
                                : Restriction
                                        .builder().and(
                                                Restriction.builder()
                                                        .let(BusinessEntity.Transaction,
                                                                InterfaceName.TransactionDate.name())
                                                        .lte(range.getRight()).build(),
                                                Restriction.builder()
                                                        .let(BusinessEntity.Transaction,
                                                                InterfaceName.TransactionDate.name())
                                                        .gte(range.getLeft()).build())
                                        .build())
                .build();
        Query query = Query.builder()
                .select(new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name())).distinct(true)
                .from(BusinessEntity.Transaction).where(restriction).build();
        long totalCount = queryEvaluatorService.getCount(attrRepo, query, sqlUser);
        log.info("totalCount " + totalCount);

        // valid Purchased
        Bucket.Transaction txn = new Bucket.Transaction(prodId, timeFilter, null, null, false);
        long everCount = countTxnBkt(txn, sqlUser);
        testAndAssertCount(sqlUser, totalCount, expectedTotal);
        log.info("everCount " + everCount);

        // invalid Purchased
        restriction = Restriction.builder() //
                .and(Restriction.builder().let(BusinessEntity.Transaction, InterfaceName.ProductId.name()).eq(prodId)
                        .build(), //
                        Restriction.builder().let(BusinessEntity.Transaction, InterfaceName.TotalAmount.name()).lte("0")
                                .build(), //
                        Restriction.builder().let(BusinessEntity.Transaction, InterfaceName.TotalQuantity.name())
                                .lte("0").build(), //
                        range == null ? null
                                : Restriction
                                        .builder().and(
                                                Restriction.builder()
                                                        .let(BusinessEntity.Transaction,
                                                                InterfaceName.TransactionDate.name())
                                                        .lte(range.getRight()).build(),
                                                Restriction.builder()
                                                        .let(BusinessEntity.Transaction,
                                                                InterfaceName.TransactionDate.name())
                                                        .gte(range.getLeft()).build())
                                        .build()) //
                .build();
        query = Query.builder().select(new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name()))
                .distinct(true).from(BusinessEntity.Transaction).where(restriction).build();
        long invalidPurchaseCount = queryEvaluatorService.getCount(attrRepo, query, sqlUser);
        // testAndAssertCount(sqlUser, totalCount, expectedTotal);
        log.info("invalidEverCount " + invalidPurchaseCount);

        // check add up
        Assert.assertEquals(totalCount, everCount + invalidPurchaseCount);

        // 2. Ever, Amount > 0, Quantity > 0
        AggregationFilter greaterThan0 = new AggregationFilter(ComparisonType.GREATER_THAN,
                Collections.singletonList(0));
        txn = new Bucket.Transaction(prodId, timeFilter, greaterThan0, greaterThan0, false);
        long count = countTxnBkt(txn, sqlUser);
        Assert.assertTrue(count <= totalCount, String.format("%d <= %d", count, totalCount));
        Assert.assertTrue(count <= everCount, String.format("%d <= %d", count, totalCount));
        log.info("count " + count);

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

    private long countTxnBkt(Bucket.Transaction txn, String sqlUser) {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction restriction = getTxnRestriction(txn);
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        return entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue, sqlUser);
    }

    private Restriction getTxnRestriction(Bucket.Transaction txn) {
        Bucket bucket = Bucket.txnBkt(txn);
        return new BucketRestriction(TRANSACTION_LOOKUP, bucket);
    }

    @DataProvider(name = "timefilterProvider")
    private Object[][] timefilterProvider() {
        return getTimeFilterDataProvider();
    }

    protected Object[][] getTimeFilterDataProvider() {
        TimeFilter betweendates = new TimeFilter( //
                ComparisonType.BETWEEN_DATE, //
                PeriodStrategy.Template.Date.name(), //
                Arrays.asList("2017-07-15", "2017-08-15"));
        return new Object[][] { //
                { SEGMENT_USER, TimeFilter.ever(), 782L }, //
                { SEGMENT_USER, betweendates, 183 } //
        };
    }

}
