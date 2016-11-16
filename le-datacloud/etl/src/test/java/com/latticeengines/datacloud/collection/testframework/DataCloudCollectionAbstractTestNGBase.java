package com.latticeengines.datacloud.collection.testframework;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.sqoop.exposed.service.SqoopJobService;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-datacloud-collection-context.xml" })
public abstract class DataCloudCollectionAbstractTestNGBase extends AbstractTestNGSpringContextTests {

    @Value("${datacloud.test.env}")
    protected String testEnv;

    @Value("${datacloud.collection.host}")
    private String dbHost;

    @Value("${datacloud.collection.port}")
    private int dbPort;

    @Value("${datacloud.collection.db}")
    private String db;

    @Value("${datacloud.user}")
    private String dbUser;

    @Value("${datacloud.password.encrypted}")
    private String dbPassword;

    @Value("${datacloud.collection.sqoop.mapper.number}")
    private int numMappers;

    @Autowired
    protected HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    private SqoopJobService sqoopService;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    @Qualifier(value = "propDataCollectionJdbcTemplate")
    protected JdbcTemplate jdbcTemplateCollectionDB;

    @Autowired
    @Qualifier(value = "propDataBulkJdbcTemplate")
    protected JdbcTemplate jdbcTemplateBulkDB;

    protected void uploadAvroToCollectionDB(String avroDir, String destTable) {
        String assignedQueue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
        String customer = "TestSqoopAgent";

        truncateJdbcTableIfExists(destTable);

        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(dbHost).port(dbPort).db(db).user(dbUser).encryptedPassword(CipherUtils.encrypt(dbPassword));
        DbCreds creds = new DbCreds(builder);
        // sqoopService.exportDataSync(destTable, avroDir, creds, assignedQueue, customer + "-upload-" + destTable, numMappers, null);
    }

    @SuppressWarnings("unused")
    private void dropJdbcTableIfExists(String tableName) {
        jdbcTemplateCollectionDB.execute("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'"
                + tableName + "') AND type in (N'U')) DROP TABLE " + tableName);
    }

    private void truncateJdbcTableIfExists(String tableName) {
        jdbcTemplateCollectionDB.execute("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'"
                + tableName + "') AND type in (N'U')) TRUNCATE TABLE " + tableName);
    }

}
