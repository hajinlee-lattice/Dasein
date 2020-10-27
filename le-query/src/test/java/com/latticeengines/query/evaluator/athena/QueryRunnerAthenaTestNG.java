package com.latticeengines.query.evaluator.athena;

import static com.latticeengines.query.factory.AthenaQueryProvider.ATHENA_USER;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.query.evaluator.QueryRunnerTestNG;

public class QueryRunnerAthenaTestNG extends QueryRunnerTestNG implements RedshiftAndAthenaQueryTester {

    private static final Logger log = LoggerFactory.getLogger(QueryRunnerAthenaTestNG.class);

    private static final String BITENCODED_NOMINAL_ATTR = "TechIndicator_EmailCampaigns";

    @Override
    protected String getBitEncodedNominalAttr() {
        return BITENCODED_NOMINAL_ATTR;
    }

    @Inject
    private AthenaQueryTester queryTester;

    @BeforeClass(groups = "functional")
    public void setupBase() {
        initializeAttributeRepo(3, false);
        setupQueryTester(customerSpace, attrRepo);
    }

    @AfterClass(groups = "functional", alwaysRun = true)
    public void teardown() {
        teardownQueryTester();
    }

    @DataProvider(name = "userContexts")
    private Object[][] provideSqlUserContexts() {
        return new Object[][] {
                { SQL_USER, "Redshift" },
                { ATHENA_USER, "Athena" }
        };
    }

    @DataProvider(name = "bitEncodedData")
    private Object[][] provideBitEncodedDataWithSparkUser() {
        Object[][] basicTests = super.getBitEncodedTestData();

        Object[][] basicTestsWithMultipleUsers = new Object[basicTests.length*2][];
        for (int i=0; i<basicTests.length; i++) {
            basicTestsWithMultipleUsers[i*2] = basicTests[i];
            // Add SparkUser for the same set of data
            Object[] sparkUserTestCase = Arrays.copyOf(basicTests[i], basicTests[i].length);
            sparkUserTestCase[0] = ATHENA_USER;
            basicTestsWithMultipleUsers[(i*2)+1] = sparkUserTestCase;
        }
        log.info("Test Data Counts from Base Class: {}, From Current Dataprovider: {}", //
                basicTests.length, basicTestsWithMultipleUsers.length);
        return basicTestsWithMultipleUsers;
    }

    @Override
    public Logger getLogger() {
        return log;
    }

    @Override
    public AthenaQueryTester getQueryTester() {
        return queryTester;
    }

    @Override
    protected long testGetCountAndAssert(String sqlUser, Query query, long expectedCount) {
        return testGetCountAndAssertFromTester(sqlUser, query, expectedCount);
    }

    @Override
    protected List<Map<String, Object>> testGetDataAndAssert(String sqlUser, Query query, long expectedCount, List<Map<String, Object>> expectedResult) {
        return testGetDataAndAssertFromTester(sqlUser, query, expectedCount, expectedResult);
    }

    @Override
    public long getCountFromRedshift(AttributeRepository attrRepo, Query query, String sqlUser) {
        return queryEvaluatorService.getCount(attrRepo, query, sqlUser);
    }

    @Override
    public DataPage getDataFromRedshift(AttributeRepository attrRepo, Query query, String sqlUser) {
        return queryEvaluatorService.getData(attrRepo, query, sqlUser);
    }

}
