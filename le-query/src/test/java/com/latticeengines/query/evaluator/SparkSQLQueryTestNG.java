package com.latticeengines.query.evaluator;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.hadoop.exposed.service.EMRCacheService;
import com.latticeengines.query.exposed.service.SparkSQLService;
import com.latticeengines.query.functionalframework.QueryFunctionalTestNGBase;
import com.latticeengines.spark.exposed.service.LivySessionService;
import com.mchange.v2.util.CollectionUtils;

public class SparkSQLQueryTestNG extends QueryFunctionalTestNGBase {

    private static final class Attrs {
        static final String AccountId = InterfaceName.AccountId.name();
        static final String Website = InterfaceName.Website.name();

        static final String LDC_Name = "LDC_Name";
        static final String LDC_City = "LDC_City";
        static final String LDC_State = "LDC_State";
        static final String LDC_Country = "LDC_Country";
    }

    @Inject
    private LivySessionService sessionService;

    @Inject
    private EMRCacheService emrCacheService;

    @Inject
    private SparkSQLService sparkSQLService;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    private LivySession session;
    private int reuseLivySession = 0; // set the session id to reuse.

    @BeforeClass(groups = "functional")
    public void setupBase() {
        initializeAttributeRepo(3);
        if (reuseLivySession > 0) {
            reuseLivyEnvironment(reuseLivySession);
        } else {
            setupLivyEnvironment();
        }
    }

    @AfterClass(groups = "functional", alwaysRun = true)
    public void teardown() {
        if (reuseLivySession == 0) {
            // comment out this statement to reuse the livy session in next run
            sessionService.stopSession(session);
        }
    }

    @Test(groups = "functional")
    public void testCount() {
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Account, Attrs.LDC_Country).eq("USA") //
                .build();
        Query query = Query.builder() //
                .select(BusinessEntity.Account) //
                .where(restriction).build();
        long redshiftCount = getCountFromRedshift(query);
        Assert.assertEquals(redshiftCount, 17958);
        long sparkCount = getCountFromSpark(query);
        Assert.assertEquals(sparkCount, redshiftCount);
    }

    @Test(groups = "functional")
    public void testSelect() {
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Account, Attrs.AccountId).eq("0011400001jpK6BAAU") //
                .build();
        Query query = Query.builder() //
                .select(BusinessEntity.Account, Attrs.Website) //
                .select(BusinessEntity.Account, Attrs.LDC_Name, Attrs.LDC_City, Attrs.LDC_State, Attrs.LDC_Country) //
                .where(restriction).build();
        List<Map<String, Object>> results = getDataFromRedshift(query);
        Assert.assertEquals(results.size(), 1);
        Map<String, Object> redshiftRow = results.get(0);
        Assert.assertEquals(redshiftRow.size(), 5);
        Assert.assertNotNull(redshiftRow.get(Attrs.Website));
        Assert.assertEquals(redshiftRow.get(Attrs.LDC_Name).toString(), "New York University Medical Center");
        Assert.assertEquals(redshiftRow.get(Attrs.LDC_City).toString(), "New York");
        Assert.assertEquals(redshiftRow.get(Attrs.LDC_State).toString().toUpperCase(), "NEW YORK");
        Assert.assertEquals(redshiftRow.get(Attrs.LDC_Country).toString(), "USA");

        HdfsDataUnit sparkResult = getDataFromSpark(query);
        Assert.assertEquals(sparkResult.getCount(), Long.valueOf(1)); // spark result has count
        String avroPath = sparkResult.getPath();
        AvroUtils.AvroFilesIterator iterator = AvroUtils.avroFileIterator(yarnConfiguration, avroPath + "/*.avro");
        iterator.forEachRemaining(record -> {
            Assert.assertEquals(CollectionUtils.size(record.getSchema().getFields()), redshiftRow.size());
            Assert.assertEquals(record.get(Attrs.Website).toString(), redshiftRow.get(Attrs.Website).toString());
            Assert.assertEquals(record.get(Attrs.LDC_Name).toString(), redshiftRow.get(Attrs.LDC_Name).toString());
            Assert.assertEquals(record.get(Attrs.LDC_City).toString(), redshiftRow.get(Attrs.LDC_City).toString());
            Assert.assertEquals(record.get(Attrs.LDC_State).toString(), redshiftRow.get(Attrs.LDC_State).toString());
            Assert.assertEquals(record.get(Attrs.LDC_Country).toString(), redshiftRow.get(Attrs.LDC_Country).toString());
        });
    }

    private long getCountFromRedshift(Query query) {
        // queryEvaluatorService.getData may have side effect to the query object
        Query clonedQuery = query.getDeepCopy();
        return queryEvaluatorService.getCount(attrRepo, clonedQuery, SQL_USER);
    }

    private List<Map<String, Object>> getDataFromRedshift(Query query) {
        // queryEvaluatorService.getData may have side effect to the query object
        Query clonedQuery = query.getDeepCopy();
        return queryEvaluatorService.getData(attrRepo, clonedQuery, SQL_USER).getData();
    }

    private long getCountFromSpark(Query query) {
        // queryEvaluatorService.getQueryStr may have side effect to the query object
        Query clonedQuery = query.getDeepCopy();
        //TODO: to be replaced by a spark-oriented sql string
        String sql = queryEvaluatorService.getQueryStr(attrRepo, clonedQuery, SQL_USER);
        return sparkSQLService.getCount(customerSpace, session, sql);
    }

    private HdfsDataUnit getDataFromSpark(Query query) {
        // queryEvaluatorService.getQueryStr may have side effect to the query object
        Query clonedQuery = query.getDeepCopy();
        //TODO: to be replaced by a spark-oriented sql string
        String sql = queryEvaluatorService.getQueryStr(attrRepo, clonedQuery, SQL_USER);
        return sparkSQLService.getData(customerSpace, session, sql);
    }

    private void setupLivyEnvironment() {
        session = sparkSQLService.initializeLivySession(attrRepo, tblPathMap);
    }

    private void reuseLivyEnvironment(int sessionId) {
        String livyHost;
        if (Boolean.TRUE.equals(useEmr)) {
            livyHost = emrCacheService.getLivyUrl();
        } else {
            livyHost = "http://localhost:8998";
        }
        session = sessionService.getSession(new LivySession(livyHost, sessionId));
    }

}
