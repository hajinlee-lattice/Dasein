package com.latticeengines.objectapi.util;

import static com.latticeengines.domain.exposed.util.RestrictionUtils.TRANSACTION_LOOKUP;
import static com.latticeengines.query.factory.SparkQueryProvider.SPARK_BATCH_USER;

import java.util.Collections;

import javax.inject.Inject;

import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.LogicalOperator;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.util.TimeFilterTranslator;
import com.latticeengines.objectapi.service.TransactionService;
import com.latticeengines.objectapi.service.impl.QueryServiceImplTestNGBase;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;
import com.latticeengines.query.exposed.factory.QueryFactory;
import com.querydsl.sql.SQLQuery;

public class QueryTranslatorTestNG extends QueryServiceImplTestNGBase {

    @Inject
    private QueryEvaluatorService queryEvaluatorService;

    @Inject
    private TransactionService transactionService;

    @BeforeClass(groups = "functional")
    public void setup() {
        super.setupTestData(2);
    }

    @Test(groups = "functional")
    public void testTranslateTxn() {
        MultiTenantContext.setTenant(tenant);
        QueryFactory queryFactory = queryEvaluatorService.getQueryFactory();
        TimeFilterTranslator timeTranslator = transactionService.getTimeFilterTranslator(maxTransactionDate);
        TimeFilter prior2Months = new TimeFilter( //
                ComparisonType.PRIOR_ONLY, //
                PeriodStrategy.Template.Month.name(), //
                Collections.singletonList(2));
        String prodId = "o13brsbfF10fllM6VUZRxMO7wfo5I7Ks";
        Bucket.Transaction txn = new Bucket.Transaction(prodId, prior2Months, null, null, false);
        EntityQueryTranslator translator = new EntityQueryTranslator(queryFactory, attrRepo);
        FrontEndQuery feQuery = toFrontEndQuery(txn);
        Query query = translator.translateEntityQuery(feQuery, attrRepo, true, timeTranslator, //
                "segment");
        Assert.assertTrue(query.getRestriction() instanceof LogicalRestriction);
        LogicalRestriction logical = (LogicalRestriction) query.getRestriction();
        Assert.assertEquals(logical.getRestrictions().size(), 2);
        Assert.assertEquals(logical.getOperator(), LogicalOperator.AND);
    }

    @Test(groups = "functional")
    public void testUnencodedBoolean() {
        String fakeBoolean = "Fake_Boolean";
        ColumnMetadata cm = attrRepo.getColumnMetadata(new AttributeLookup(BusinessEntity.Account, fakeBoolean));
        System.out.println("Fake_Boolean: " + JsonUtils.serialize(cm));

        MultiTenantContext.setTenant(tenant);
        QueryFactory queryFactory = queryEvaluatorService.getQueryFactory();
        EntityQueryTranslator translator = new EntityQueryTranslator(queryFactory, attrRepo);

        TimeFilterTranslator timeTranslator = transactionService.getTimeFilterTranslator(maxTransactionDate);

        AttributeLookup attrLp = new AttributeLookup(BusinessEntity.Account, fakeBoolean);
        // AttributeLookup attrLp = new AttributeLookup(BusinessEntity.Account,
        // "TechIndicator_WhatCounts");
        Bucket bkt1 = Bucket.valueBkt("Yes");
        BucketRestriction bktRes1 = new BucketRestriction(attrLp, bkt1);
        Bucket bkt2 = Bucket.valueBkt("No");
        BucketRestriction bktRes2 = new BucketRestriction(attrLp, bkt2);
        Restriction logical = Restriction.builder().or(bktRes1, bktRes2).build();

        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setAccountRestriction(new FrontEndRestriction(logical));

        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);

        String sqlUser = SPARK_BATCH_USER;
        Query query = translator.translateEntityQuery(frontEndQuery, attrRepo, true, timeTranslator, sqlUser);

        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        sqlQuery.setUseLiterals(true);
        String sql = sqlQuery.getSQL().getSQL();
        Assert.assertTrue(sql.contains(String.format("Account.%s = true or Account.%s = false", fakeBoolean, fakeBoolean)));
        Assert.assertFalse(sql.contains(String.format("lower('Account.%s') = lower('yes')", fakeBoolean)));
        Assert.assertFalse(sql.contains(String.format("lower('Account.%s') = lower('no')", fakeBoolean)));
    }

    private FrontEndQuery toFrontEndQuery(Bucket.Transaction txn) {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Bucket bucket = Bucket.txnBkt(txn);
        Restriction restriction = new BucketRestriction(TRANSACTION_LOOKUP, bucket);
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        return frontEndQuery;
    }

}
