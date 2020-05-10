package com.latticeengines.objectapi.service.impl.sparksql;

import static com.latticeengines.query.evaluator.sparksql.SparkSQLTestInterceptor.SPARK_TEST_GROUP;
import static com.latticeengines.query.factory.SparkQueryProvider.SPARK_BATCH_USER;

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

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.objectapi.service.EntityQueryService;
import com.latticeengines.objectapi.service.impl.EntityQueryServiceImplBigListTestNG;
import com.latticeengines.objectapi.service.sparksql.impl.EntityQueryServiceSparkSQLImpl;
import com.latticeengines.query.evaluator.sparksql.SparkSQLQueryTester;
import com.latticeengines.query.evaluator.sparksql.SparkSQLTestInterceptor;

@Listeners(SparkSQLTestInterceptor.class)
public class EntityQueryServiceImplBigListSparkSQLTestNG extends EntityQueryServiceImplBigListTestNG //
        implements RedshiftAndSparkQueryObjectAPITester {

    private static final Logger log = LoggerFactory.getLogger(EntityQueryServiceImplBigListSparkSQLTestNG.class);

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

    @DataProvider(name = "userContexts", parallel = true)
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
    public void testBigList() {
        FrontEndQuery frontEndQuery = getBigListFrontEndQuery();
        String sql = entityQueryServiceSparkSQL.getQueryStr(frontEndQuery, DataCollection.Version.Blue,
                SPARK_BATCH_USER, true);
        log.info("spark sql query is: {}" + sql);
        long count = entityQueryServiceSparkSQL.getCount(frontEndQuery, DataCollection.Version.Blue,
                SPARK_BATCH_USER);
        sql = entityQueryService.getQueryStr(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER, true);
        log.info("redshift query is: {}" + sql);
        long count2 = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue,
                SEGMENT_USER);
        Assert.assertEquals(count2, count);
    }

}
