package com.latticeengines.query.evaluator.sparksql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Inject;

import org.apache.avro.Schema.Field;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.testng.Assert;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
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

    private static final Logger log = LoggerFactory.getLogger(SparkSQLQueryTester.class);

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

    @Inject
    protected Configuration yarnConfiguration;

    private LivySession session;
    private int reuseLivySession = 0; // set the session id to reuse.
    private boolean sessionIsReused = false;

    protected AttributeRepository attrRepo;
    protected Map<String, String> tblPathMap;
    protected CustomerSpace customerSpace;
    private AtomicInteger executionCounter;

    public AttributeRepository getAttrRepo() {
        return attrRepo;
    }

    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    public LivySession getLivySession() {
        return session;
    }

    public void setupTestContext(CustomerSpace customerSpace, AttributeRepository attrRepo, Map<String, String> tblPathMap) {
        this.customerSpace = customerSpace;
        this.attrRepo = attrRepo;
        this.tblPathMap = tblPathMap;

        if (reuseLivySession > 0) {
            sessionIsReused = true;
            reuseLivyEnvironment(reuseLivySession);
        } else {
            setupLivyEnvironment();
        }

        String trxnTable = attrRepo.getTableName(TableRoleInCollection.AggregatedPeriodTransaction);
        sparkSQLService.prepareForCrossSellQueries(session, "Month", trxnTable, "MEMORY_AND_DISK_SER");
    }

    private void setupLivyEnvironment() {
        session = sparkSQLService.initializeLivySession(attrRepo, tblPathMap, 1, //
                "MEMORY_AND_DISK_SER", null);
        // comment out this statement to reuse the livy session in next run
        Runtime.getRuntime().addShutdownHook(new Thread(() -> sessionService.stopSession(session)));
        executionCounter = new AtomicInteger(0);
    }

    private void reuseLivyEnvironment(int sessionId) {
        String livyHost;
        if (Boolean.TRUE.equals(useEmr)) {
            livyHost = emrCacheService.getLivyUrl();
        } else {
            livyHost = "http://localhost:8998";
        }
        session = sessionService.getSession(new LivySession(livyHost, sessionId));
        executionCounter = new AtomicInteger(0);
    }

    public void teardown() {
        if (reuseLivySession == 0) {
            // comment out this statement to reuse the livy session in next run
            sessionService.stopSession(session);
        }
    }

    public void refreshLivySession() {
        teardown();
        setupLivyEnvironment();
        String trxnTable = attrRepo.getTableName(TableRoleInCollection.AggregatedPeriodTransaction);
        sparkSQLService.prepareForCrossSellQueries(session, "Month", trxnTable, "MEMORY_AND_DISK_SER");
    }

    private void incrementExecutionCounter() {
        log.info("Livy session {} has been used for {} times.", //
                session.getSessionId(), executionCounter.incrementAndGet());
    }

    public long getCountFromSpark(Query query) {
        String sql1 = queryEvaluatorService.getQueryStr(attrRepo, query, SparkQueryProvider.SPARK_BATCH_USER);
        long count1 = sparkSQLService.getCount(customerSpace, session, sql1);
        incrementExecutionCounter();
        // test idempotent
        String sql2 = queryEvaluatorService.getQueryStr(attrRepo, query, SparkQueryProvider.SPARK_BATCH_USER);
        long count2 = sparkSQLService.getCount(customerSpace, session, sql2);
        incrementExecutionCounter();
        Assert.assertEquals(count1, count2);
        return count1;
    }

    public HdfsDataUnit getDataFromSpark(Query query) {
        String sql = queryEvaluatorService.getQueryStr(attrRepo, query, SparkQueryProvider.SPARK_BATCH_USER);
        HdfsDataUnit dataUnit = sparkSQLService.getData(customerSpace, session, sql, null);
        incrementExecutionCounter();
        return dataUnit;
    }

    public int getCurrentSessionUsage() {
        return executionCounter.get();
    }

    public List<Map<String, Object>> convertHdfsDataUnitToList(HdfsDataUnit sparkResult) {
        List<Map<String, Object>> resultData = new ArrayList<>();
        String avroPath = sparkResult.getPath();
        AvroUtils.AvroFilesIterator iterator = AvroUtils.iterateAvroFiles(yarnConfiguration, avroPath + "/*.avro");
        iterator.forEachRemaining(record -> {
            Map<String, Object> row = new HashMap<>();
            for (Field field: record.getSchema().getFields()) {
                Object value = record.get(field.name());
                if (value instanceof Utf8) {
                    value = ((Utf8)value).toString();
                }
                row.put(field.name(), value);
            }
            resultData.add(row);
        });
        return resultData;
    }

}
