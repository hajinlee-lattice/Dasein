package com.latticeengines.eai.service.impl.redshift;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.eai.EaiJob;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.eai.exposed.service.EaiService;
import com.latticeengines.eai.functionalframework.EaiMiniClusterFunctionalTestNGBase;
import com.latticeengines.eai.service.EaiYarnService;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;

public class HdfsToRedshiftServiceImplTestNG extends EaiMiniClusterFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(HdfsToRedshiftServiceImplTestNG.class);

    private static final String HDFS_DIR = "/tmp/hdfs2sf";
    private static final String FILENAME = "uncompressed.avro";

    private static final String TEST_TABLE = "CAMEL_TEST";

    @Autowired
    private HdfsToRedshiftService hdfsToRedshiftService;

    @Autowired
    private EaiYarnService eaiYarnService;

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
        super.setup();
        testTable = leStack + "_" + TEST_TABLE;
        cleanup();
        URL url = ClassLoader.getSystemResource("com/latticeengines/eai/service/impl/camel/uncompressed.avro");
        log.info("Uploading test avro to hdfs.");
        HdfsUtils.copyFromLocalToHdfs(miniclusterConfiguration, url.getPath(), HDFS_DIR + "/" + FILENAME);
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        cleanup();
        super.clear();
    }

    @Test(groups = "functional")
    public void testUploadToRedshift() throws Exception {
        HdfsToRedshiftConfiguration configuration = getExportConfiguration();
        EaiJob job = eaiYarnService.createJob(configuration);

        ApplicationId appId = testYarnJob(job.getClient(), job.getAppMasterPropertiesObject(),
                job.getContainerPropertiesObject());

        waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        verify(configuration);
    }

    private void verify(HdfsToRedshiftConfiguration configuration) {
        String table = configuration.getRedshiftTableConfiguration().getTableName();
        String sql = String.format("SELECT * FROM %s LIMIT 10", table);
        List<Map<String, Object>> results = redshiftJdbcTemplate.queryForList(sql);
        Assert.assertTrue(results.size() > 0, "Got 0 result by querying [" + sql + "]");

        sql = String.format("SELECT * FROM %s LIMIT 10", table);
        results = redshiftJdbcTemplate.queryForList(sql);
        Assert.assertTrue(results.size() > 0, "Got 0 result by querying [" + sql + "]");
    }

    private void cleanup() throws Exception {
        HdfsUtils.rmdir(miniclusterConfiguration, HDFS_DIR);
        HdfsToRedshiftConfiguration configuration = getExportConfiguration();
        hdfsToRedshiftService.cleanupS3(configuration);
        String table = configuration.getRedshiftTableConfiguration().getTableName();
        redshiftService.dropTable(table);
        FileUtils.deleteQuietly(new File("tmp"));
    }

    private HdfsToRedshiftConfiguration getExportConfiguration() {
        HdfsToRedshiftConfiguration configuration = new HdfsToRedshiftConfiguration();
        configuration.setExportInputPath(HDFS_DIR + "/*.avro");
        configuration.setCleanupS3(true);
        configuration.setCreateNew(true);
        configuration.setAppend(true);
        configuration.setNoSplit(true);
        RedshiftTableConfiguration redshiftTableConfiguration = new RedshiftTableConfiguration();
        redshiftTableConfiguration.setTableName(testTable);
        redshiftTableConfiguration.setJsonPathPrefix("uncompressed.jsonpath");
        redshiftTableConfiguration.setS3Bucket(s3Bucket);
        configuration.setRedshiftTableConfiguration(redshiftTableConfiguration);
        return configuration;
    }

}
