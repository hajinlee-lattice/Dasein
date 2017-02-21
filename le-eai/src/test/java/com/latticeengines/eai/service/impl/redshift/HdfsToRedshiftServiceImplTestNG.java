package com.latticeengines.eai.service.impl.redshift;

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
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.eai.exposed.service.EaiService;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;

public class HdfsToRedshiftServiceImplTestNG extends EaiFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(HdfsToRedshiftServiceImplTestNG.class);

    private static final String HDFS_DIR = "/tmp/hdfs2sf";
    private static final String FILENAME = "camel.avro";

    private static final String TEST_TABLE = "CAMEL_TEST";

    @Autowired
    private HdfsToRedshiftService hdfsToRedshiftService;

    @Value("${common.le.stack}")
    private String leStack;

    @Value("${common.le.environment}")
    private String leEnvironment;

    @Value("${aws.test.s3.bucket}")
    private String s3Bucket;

    @Autowired
    @Qualifier(value = "redshiftJdbcTemplate")
    private JdbcTemplate redshiftJdbcTemplate;

    @Autowired
    private RedshiftService redshiftService;
    
    @SuppressWarnings("unused")
    @Autowired
    private EaiService eaiService;

    private String testTable;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        testTable = leStack + "_" + TEST_TABLE;
        hdfsToRedshiftService.setS3Bucket(s3Bucket);
        cleanup();
        InputStream avroStream = ClassLoader
                .getSystemResourceAsStream("com/latticeengines/eai/service/impl/camel/camel.avro");
        log.info("Uploading test avro to hdfs.");
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, avroStream, HDFS_DIR + "/" + FILENAME);
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        cleanup();
    }

    @Test(groups = "functional")
    public void testUploadToRedshift() throws Exception {
        HdfsToRedshiftConfiguration configuration = getExportConfiguration();
        hdfsToRedshiftService.uploadDataObjectToS3(configuration);
        hdfsToRedshiftService.copyToRedshift(configuration);
        //eaiService.exportDataFromHdfs(configuration);
        verify(configuration);
    }

    private void verify(HdfsToRedshiftConfiguration configuration) {
        String table = configuration.getTableName();
        String sql = String.format("SELECT * FROM %s LIMIT 10", table);
        List<Map<String, Object>> results = redshiftJdbcTemplate.queryForList(sql);
        Assert.assertTrue(results.size() > 0, "Got 0 result by querying [" + sql + "]");

        sql = String.format("SELECT * FROM %s LIMIT 10", table);
        results = redshiftJdbcTemplate.queryForList(sql);
        Assert.assertTrue(results.size() > 0, "Got 0 result by querying [" + sql + "]");
    }

    private void cleanup() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, HDFS_DIR);
        HdfsToRedshiftConfiguration configuration = getExportConfiguration();
        hdfsToRedshiftService.cleanupS3(configuration);
        String table = configuration.getTableName();
        redshiftService.dropTable(table);
        FileUtils.deleteQuietly(new File("tmp"));
    }

    private HdfsToRedshiftConfiguration getExportConfiguration() {
        HdfsToRedshiftConfiguration configuration = new HdfsToRedshiftConfiguration();
        configuration.setExportInputPath(HDFS_DIR + "/*.avro");
        configuration.setTableName(testTable);
        configuration.setJsonPathPrefix("camel.jsonpath");
        return configuration;
    }

}
