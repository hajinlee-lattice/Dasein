package com.latticeengines.query.evaluator.sparksql;

import java.util.Map;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.hadoop.exposed.service.EMRCacheService;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;
import com.latticeengines.query.exposed.service.SparkSQLService;
import com.latticeengines.query.factory.SparkQueryProvider;
import com.latticeengines.spark.exposed.service.LivySessionService;

@Component
public class SparkSQLQueryTester {

    @Inject
    private LivySessionService sessionService;

    @Inject
    private EMRCacheService emrCacheService;

    @Inject
    private SparkSQLService sparkSQLService;

    @Inject
    protected QueryEvaluatorService queryEvaluatorService;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    private LivySession session;
    private int reuseLivySession = 0; // set the session id to reuse.

    private AttributeRepository attrRepo;
    protected Map<String, String> tblPathMap;
    protected CustomerSpace customerSpace;

    public void setupTestContext(CustomerSpace customerSpace, AttributeRepository attrRepo, Map<String, String> tblPathMap) {
        this.customerSpace = customerSpace;
        this.attrRepo = attrRepo;
        this.tblPathMap = tblPathMap;

        if (reuseLivySession > 0) {
            reuseLivyEnvironment(reuseLivySession);
        } else {
            setupLivyEnvironment();
        }
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

    public void teardown() {
        if (reuseLivySession == 0) {
            // comment out this statement to reuse the livy session in next run
            sessionService.stopSession(session);
        }
    }

    public long getCountFromSpark(Query query) {
        String sql = queryEvaluatorService.getQueryStr(attrRepo, query, SparkQueryProvider.SPARK_BATCH_USER);
        return sparkSQLService.getCount(customerSpace, session, sql);
    }

    public HdfsDataUnit getDataFromSpark(Query query) {
        String sql = queryEvaluatorService.getQueryStr(attrRepo, query, SparkQueryProvider.SPARK_BATCH_USER);
        return sparkSQLService.getData(customerSpace, session, sql);
    }

    public long getCountFromSpark(String queryString) {
        return sparkSQLService.getCount(customerSpace, session, queryString);
    }

    public HdfsDataUnit getDataFromSpark(String queryString) {
        return sparkSQLService.getData(customerSpace, session, queryString);
    }

}
