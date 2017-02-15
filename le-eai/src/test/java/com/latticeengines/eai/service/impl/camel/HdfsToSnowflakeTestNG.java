package com.latticeengines.eai.service.impl.camel;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.eai.HdfsToSnowflakeConfiguration;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.eai.service.impl.snowflake.HdfsToSnowflakeService;
import com.latticeengines.snowflakedb.exposed.service.SnowflakeService;
import com.latticeengines.snowflakedb.exposed.util.SnowflakeUtils;

public class HdfsToSnowflakeTestNG extends EaiFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(HdfsToSnowflakeTestNG.class);

    private static final String HDFS_DIR = "/tmp/hdfs2sf";
    private static final String FILENAME = "camel.avro";

    private static final String TEST_DB = "TESTDB";
    private static final String TEST_TABLE = "CAMEL_TEST";

    @Autowired
    private HdfsToSnowflakeService hdfsToSnowflakeService;

    @Value("${common.le.stack}")
    private String leStack;

    @Value("${common.le.environment}")
    private String leEnvironment;

    @Value("${aws.test.s3.bucket}")
    private String s3Bucket;

    @Autowired
    @Qualifier(value = "snowflakeJdbcTemplate")
    private JdbcTemplate snowflakeJdbcTemplate;

    @Autowired
    private SnowflakeService snowflakeService;

    private String testDB;
    private String testTable;

    @BeforeClass(groups = "aws")
    public void setup() throws Exception {
        testDB = leEnvironment + "_" + leStack + "_" + TEST_DB;
        testTable = leStack + "_" + TEST_TABLE;
        hdfsToSnowflakeService.setS3Bucket(s3Bucket);
        snowflakeService.createDatabase(testDB, s3Bucket);
        cleanup();
        InputStream avroStream = ClassLoader
                .getSystemResourceAsStream("com/latticeengines/eai/service/impl/camel/camel.avro");
        log.info("Uploading test avro to hdfs.");
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, avroStream, HDFS_DIR + "/" + FILENAME);
    }

    @AfterClass(groups = "aws")
    public void teardown() throws Exception {
        cleanup();
    }

    @Test(groups = "aws")
    public void testUploadToSnowflake() throws Exception {
        HdfsToSnowflakeConfiguration configuration = getExportConfiguration();
        hdfsToSnowflakeService.uploadToS3(configuration);
        hdfsToSnowflakeService.copyToSnowflake(configuration);
        // routeService.cleanupS3(configuration);
        verify(configuration);
    }

    private void verify(HdfsToSnowflakeConfiguration configuration) {
        String db = configuration.getDb();
        String table = configuration.getTableName();
        String sql = String.format("SELECT * FROM %s LIMIT 10",
                SnowflakeUtils.toQualified(db, SnowflakeUtils.toAvroRawTable(table)));
        List<Map<String, Object>> results = snowflakeJdbcTemplate.queryForList(sql);
        Assert.assertTrue(results.size() > 0, "Got 0 result by querying [" + sql + "]");

        sql = String.format("SELECT * FROM %s LIMIT 10", SnowflakeUtils.toQualified(db, table));
        results = snowflakeJdbcTemplate.queryForList(sql);
        Assert.assertTrue(results.size() > 0, "Got 0 result by querying [" + sql + "]");
    }

    private void cleanup() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, HDFS_DIR);
        HdfsToSnowflakeConfiguration configuration = getExportConfiguration();
        hdfsToSnowflakeService.cleanupS3(configuration);
        String db = configuration.getDb();
        String table = configuration.getTableName();
        String sql = String.format("DROP TABLE IF EXISTS %s",
                SnowflakeUtils.toQualified(db, SnowflakeUtils.toAvroRawTable(table)));
        snowflakeJdbcTemplate.execute(sql);
        sql = String.format("DROP VIEW IF EXISTS %s", SnowflakeUtils.toQualified(db, table));
        snowflakeJdbcTemplate.execute(sql);
        FileUtils.deleteQuietly(new File("tmp"));
    }

    private HdfsToSnowflakeConfiguration getExportConfiguration() {
        HdfsToSnowflakeConfiguration configuration = new HdfsToSnowflakeConfiguration();
        configuration.setExportInputPath(HDFS_DIR + "/*.avro");
        configuration.setDb(testDB);
        configuration.setTableName(testTable);
        return configuration;
    }

}
