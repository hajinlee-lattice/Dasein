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

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-datacloud-collection-context.xml" })
public abstract class DataCloudCollectionAbstractTestNGBase extends AbstractTestNGSpringContextTests {

    @Value("${datacloud.test.env}")
    protected String testEnv;

    // DataCloud CollectionDB is shutdown
    @Deprecated
    @Value("${datacloud.collection.host}")
    private String dbHost;

    // DataCloud CollectionDB is shutdown
    @Deprecated
    @Value("${datacloud.collection.port}")
    private int dbPort;

    // DataCloud CollectionDB is shutdown
    @Deprecated
    @Value("${datacloud.collection.db}")
    private String db;

    // DataCloud CollectionDB is shutdown
    @Deprecated
    @Value("${datacloud.user}")
    private String dbUser;

    // DataCloud CollectionDB is shutdown
    @Deprecated
    @Value("${datacloud.password.encrypted}")
    private String dbPassword;

    @Value("${datacloud.collection.sqoop.mapper.number}")
    private int numMappers;

    @Autowired
    protected HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    // PropData CollectionDB is shutdown
    @Deprecated
    @Autowired
    @Qualifier(value = "propDataCollectionJdbcTemplate")
    protected JdbcTemplate jdbcTemplateCollectionDB;

    // PropData BulkDB is shutdown
    @Deprecated
    @Autowired
    @Qualifier(value = "propDataBulkJdbcTemplate")
    protected JdbcTemplate jdbcTemplateBulkDB;

    // PropData CollectionDB is shutdown
    @Deprecated
    protected void uploadAvroToCollectionDB(String avroDir, String destTable) {
        truncateJdbcTableIfExists(destTable);

        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(dbHost).port(dbPort).db(db).user(dbUser).encryptedPassword(CipherUtils.encrypt(dbPassword));
        // sqoopProxy.exportDataSync(destTable, avroDir, creds, assignedQueue, customer + "-upload-" + destTable, numMappers, null);
    }

    // PropData CollectionDB is shutdown
    @Deprecated
    @SuppressWarnings("unused")
    private void dropJdbcTableIfExists(String tableName) {
        jdbcTemplateCollectionDB.execute("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'"
                + tableName + "') AND type in (N'U')) DROP TABLE " + tableName);
    }

    // PropData CollectionDB is shutdown
    @Deprecated
    private void truncateJdbcTableIfExists(String tableName) {
        jdbcTemplateCollectionDB.execute("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'"
                + tableName + "') AND type in (N'U')) TRUNCATE TABLE " + tableName);
    }

}
