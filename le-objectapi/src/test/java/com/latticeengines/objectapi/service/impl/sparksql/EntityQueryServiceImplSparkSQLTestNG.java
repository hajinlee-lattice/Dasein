package com.latticeengines.objectapi.service.impl.sparksql;

import static com.latticeengines.query.evaluator.sparksql.SparkSQLTestInterceptor.SPARK_TEST_GROUP;
import static com.latticeengines.query.factory.SparkQueryProvider.SPARK_BATCH_USER;

import java.util.Arrays;
import java.util.Collections;

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
import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.objectapi.service.EntityQueryService;
import com.latticeengines.objectapi.service.impl.EntityQueryServiceImplTestNG;
import com.latticeengines.objectapi.service.sparksql.impl.EntityQueryServiceSparkSQLImpl;
import com.latticeengines.query.evaluator.sparksql.SparkSQLQueryTester;
import com.latticeengines.query.evaluator.sparksql.SparkSQLTestInterceptor;


@Listeners(SparkSQLTestInterceptor.class)
public class EntityQueryServiceImplSparkSQLTestNG extends EntityQueryServiceImplTestNG //
        implements RedshiftAndSparkQueryObjectAPITester {

    private static Logger log = LoggerFactory.getLogger(EntityQueryServiceImplSparkSQLTestNG.class);

    @Inject
    private SparkSQLQueryTester sparkSQLQueryTester;

    @Resource(name = "entityQueryServiceSparkSQL")
    private EntityQueryService entityQueryServiceSparkSQL;

    @Inject
    private EntityQueryService entityQueryService;

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

    @BeforeClass(groups = SPARK_TEST_GROUP)
    public void setupBase() {
        setupTestDataWithSpark(3);
        setupQueryTester(customerSpace, attrRepo, tblPathMap);
        ((EntityQueryServiceSparkSQLImpl) entityQueryServiceSparkSQL)
                .setLivySession(getSparkSQLQueryTester().getLivySession());
        // Init Mocks
        mockDataCollectionProxy(
                ((EntityQueryServiceSparkSQLImpl) entityQueryServiceSparkSQL).getQueryEvaluatorService());
    }

    @AfterClass(groups = SPARK_TEST_GROUP, alwaysRun = true)
    public void teardown() {
        teardownQueryTester();
    }

    @Test(groups = SPARK_TEST_GROUP)
    public void testCountInSpark() {
        FrontEndQuery frontEndQuery = loadFrontEndQueryFromResource("prior2.json");
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        String sql = entityQueryServiceSparkSQL.getQueryStr(frontEndQuery, DataCollection.Version.Blue,
                SPARK_BATCH_USER, true);
        System.out.println(sql);
        long count = entityQueryServiceSparkSQL.getCount(frontEndQuery, DataCollection.Version.Blue,
                SPARK_BATCH_USER);
        Assert.assertEquals(count, 25958L);
        long count2 = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue,
                SEGMENT_USER);
        Assert.assertEquals(count, count2);
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
        AggregationFilter greaterThan0 = new AggregationFilter(ComparisonType.GREATER_THAN,
                Collections.singletonList(0));
        Bucket.Transaction txn = new Bucket.Transaction("LHdgKhusYLJqk9JgC5SdJrwNv8tg9il", last4Years, greaterThan0, null, true);
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

        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setAccountRestriction(new FrontEndRestriction(logical3));
        frontEndQuery.setContactRestriction(new FrontEndRestriction(logical4));

        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        String sql = entityQueryServiceSparkSQL.getQueryStr(frontEndQuery, DataCollection.Version.Blue,
                SPARK_BATCH_USER, true);
        System.out.println(sql);
        long count = entityQueryServiceSparkSQL.getCount(frontEndQuery, DataCollection.Version.Blue,
                SPARK_BATCH_USER);
        Assert.assertEquals(count, 14L);
        long count2 = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue,
                SEGMENT_USER);
        Assert.assertEquals(count, count2);
    }
}
