package com.latticeengines.objectapi.service.impl.sparksql;

import static com.latticeengines.query.evaluator.sparksql.SparkSQLTestInterceptor.SPARK_TEST_GROUP;
import static com.latticeengines.query.factory.SparkQueryProvider.SPARK_BATCH_USER;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.EventType;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.objectapi.service.EventQueryService;
import com.latticeengines.objectapi.service.impl.EventQueryServiceImplTestNG;
import com.latticeengines.objectapi.service.sparksql.impl.EventQueryServiceSparkSQLImpl;
import com.latticeengines.query.evaluator.sparksql.SparkSQLQueryTester;
import com.latticeengines.query.evaluator.sparksql.SparkSQLTestInterceptor;

@Listeners(SparkSQLTestInterceptor.class)
public class EventQueryServiceImplSparkSQLTestNG extends EventQueryServiceImplTestNG
        implements RedshiftAndSparkQueryObjectAPITester {

    private static Logger log = LoggerFactory.getLogger(EventQueryServiceImplSparkSQLTestNG.class);

    @Inject
    private SparkSQLQueryTester sparkSQLQueryTester;

    @Resource(name = "eventQueryServiceSparkSQL")
    private EventQueryService eventQueryServiceSparkSql;

    @Inject
    private EventQueryService eventQueryService;

    @Override
    public Logger getLogger() {
        return log;
    }

    @Override
    public SparkSQLQueryTester getSparkSQLQueryTester() {
        return sparkSQLQueryTester;
    }

    @DataProvider(name = "userContexts", parallel = false)
    private Object[][] provideSqlUserContexts() {
        return new Object[][] { { SEGMENT_USER, "Redshift" }, { SPARK_BATCH_USER, "Spark" } };
    }

    @Override
    @BeforeClass(groups = SPARK_TEST_GROUP)
    public void setup() {
        isSparkSQLTest = true;

        setupTestDataWithSpark(3);
        setupQueryTester(customerSpace, attrRepo, tblPathMap);
        ((EventQueryServiceSparkSQLImpl) eventQueryServiceSparkSql)
                .setLivySession(getSparkSQLQueryTester().getLivySession());
        // Init Mocks
        mockDataCollectionProxy(((EventQueryServiceSparkSQLImpl) eventQueryServiceSparkSql).getQueryEvaluatorService());
    }

    @AfterClass(groups = SPARK_TEST_GROUP, alwaysRun = true)
    public void teardown() {
        teardownQueryTester();
    }

    @Override
    protected String getProductId() {
        return "eNm3Nmt72BXLZRniXJWSFO4j2jnOUpY";
    }

    @Override
    protected EventQueryService getEventQueryService(String sqlUser) {
        switch (sqlUser) {
        case SEGMENT_USER:
            return super.getEventQueryService(sqlUser);
        case SPARK_BATCH_USER:
            return eventQueryServiceSparkSql;
        }
        throw new IllegalArgumentException(String.format("SQL User: %s is not supported", sqlUser));
    }

    @Override
    protected long testAndAssertCount(String sqlUser, long resultCount, long expectedCount) {
        return testAndAssertCountFromTester(sqlUser, resultCount, expectedCount);
    }

    @Override
    protected List<Map<String, Object>> testAndAssertData(String sqlUser, List<Map<String, Object>> results,
            List<Map<String, Object>> expectedResults) {
        return testAndAssertDataFromTester(sqlUser, results, expectedResults);
    }

    @Test(groups = SPARK_TEST_GROUP)
    public void testScoringCountInSpark() {
        EventFrontEndQuery frontEndQuery = loadEventFrontEndQueryFromResource("prior.json");
        frontEndQuery.getSegmentQuery().setEvaluationDateStr(maxTransactionDate);
        String sql = eventQueryServiceSparkSql.getQueryStr(frontEndQuery, EventType.Scoring, //
                DataCollection.Version.Blue);
        System.out.println(sql);
        long count = eventQueryServiceSparkSql.getScoringCount(frontEndQuery,DataCollection.Version.Blue);
        Assert.assertEquals(count, 5692L);
        long count2 = eventQueryService.getScoringCount(frontEndQuery, DataCollection.Version.Blue);
        Assert.assertEquals(count2, count);
    }

    @Test(groups = SPARK_TEST_GROUP)
    public void testNestedNegativeInSpark() {
        // account
        AttributeLookup attrLp = new AttributeLookup(BusinessEntity.Account, "Industry");

        Bucket bkt1 = Bucket.valueBkt(ComparisonType.NOT_IN_COLLECTION, //
                Arrays.asList("MEDIA & ENTERTAINMENT", "TRAVEL, LEISURE & HOSPITALITY"));
        BucketRestriction bktRes1 = new BucketRestriction(attrLp, bkt1);
        Bucket bkt2 = Bucket.nullBkt();
        BucketRestriction bktRes2 = new BucketRestriction(attrLp, bkt2);
        Restriction logical1 = Restriction.builder().or(bktRes1, bktRes2).build();

        Bucket bkt3 = Bucket.valueBkt(ComparisonType.NOT_EQUAL, Collections.singletonList("MEDIA & ENTERTAINMENT"));
        BucketRestriction bktRes3 = new BucketRestriction(attrLp, bkt3);

        TimeFilter last4Years = new TimeFilter(attrLp, ComparisonType.WITHIN, "Year", Collections.singletonList(4));
        Bucket.Transaction txn = new Bucket.Transaction("LHdgKhusYLJqk9JgC5SdJrwNv8tg9il", last4Years, null, null, true);
        Bucket bkt4 = Bucket.txnBkt(txn);
        BucketRestriction bktRes4 = new BucketRestriction(attrLp, bkt4);

        Restriction logical2 = Restriction.builder().or(bktRes3, bktRes4).build();
        Restriction logical3 = Restriction.builder().and(logical1, logical2).build();

        // contact
        AttributeLookup attrLp2 = new AttributeLookup(BusinessEntity.Contact, "Title");
        Bucket bkt5 = Bucket.valueBkt(ComparisonType.CONTAINS, Collections.singletonList("a"));
        BucketRestriction bktRes5 = new BucketRestriction(attrLp2, bkt5);
        Bucket bkt6 = Bucket.valueBkt(ComparisonType.NOT_CONTAINS, Collections.singletonList("a"));
        BucketRestriction bktRes6 = new BucketRestriction(attrLp2, bkt6);
        Restriction logical4 = Restriction.builder().or(bktRes5, bktRes6).build();

        FrontEndQuery segmentQuery = new FrontEndQuery();
        segmentQuery.setAccountRestriction(new FrontEndRestriction(logical3));
        segmentQuery.setContactRestriction(new FrontEndRestriction(logical4));
        segmentQuery.setMainEntity(BusinessEntity.Account);
        segmentQuery.setEvaluationDateStr(maxTransactionDate);

        EventFrontEndQuery frontEndQuery = new EventFrontEndQuery();
        frontEndQuery.setSegmentQuery(segmentQuery);
        frontEndQuery.setPeriodName("Quarter");
        frontEndQuery.setTargetProductIds(Collections.singletonList("6368494B622E0CB60F9C80FEB1D0F95F"));
        frontEndQuery.setEvaluationPeriodId(211);

        String sql = eventQueryServiceSparkSql.getQueryStr(frontEndQuery, EventType.Scoring, //
                DataCollection.Version.Blue);
        System.out.println(sql);
        long count = eventQueryServiceSparkSql.getScoringCount(frontEndQuery,DataCollection.Version.Blue);
        Assert.assertEquals(count, 5692L);
        long count2 = eventQueryService.getScoringCount(frontEndQuery, DataCollection.Version.Blue);
        Assert.assertEquals(count2, count);
    }
}
