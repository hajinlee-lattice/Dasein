package com.latticeengines.propdata.collection.testframework;

import java.util.Collections;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

import com.latticeengines.dataplatform.exposed.service.SqoopSyncJobService;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.propdata.collection.service.impl.HdfsPathBuilder;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-propdata-collection-context.xml" })
public abstract class PropDataCollectionAbstractTestNGBase extends AbstractTestNGSpringContextTests {

    @Value("${propdata.test.env}")
    protected String testEnv;

    @Value("${propdata.collection.host}")
    private String dbHost;

    @Value("${propdata.collection.port}")
    private int dbPort;

    @Value("${propdata.collection.db}")
    private String db;

    @Value("${propdata.collection.user}")
    private String dbUser;

    @Value("${propdata.collection.password.encrypted}")
    private String dbPassword;

    @Value("${propdata.collection.sqoop.mapper.number}")
    private int numMappers;

    @Autowired
    protected HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    private SqoopSyncJobService sqoopService;

    protected void importFromCollectionDB(String table, String targetDir, String customer,
                                          String splitColumn, String whereClause) {
        String assignedQueue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(dbHost).port(dbPort).db(db).user(dbUser).password(dbPassword);
        DbCreds creds = new DbCreds(builder);
        if (StringUtils.isEmpty(whereClause)) {
            sqoopService.importDataSync(table, targetDir, creds, assignedQueue, customer,
                    Collections.singletonList(splitColumn), "", numMappers);
        } else {
            sqoopService.importDataSyncWithWhereCondition(
                    table, targetDir, creds, assignedQueue, customer,
                    Collections.singletonList(splitColumn), "", whereClause, numMappers);
        }
    }

}
