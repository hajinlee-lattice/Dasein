package com.latticeengines.spark.exposed.job.cdl;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.GenerateLaunchUniverseJobConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class GenerateLaunchUniverseJobTestNG extends SparkJobFunctionalTestNGBase {

    @Override
    protected String getJobName() {
        return "generateLaunchUniverse";
    }

    @Override
    protected String getScenarioName() {
        return "contactsPerAccount";
    }

    private static final Logger log = LoggerFactory.getLogger(GenerateLaunchUniverseJobTestNG.class);

    private static final String CDL_UPDATED_TIME = InterfaceName.CDLUpdatedTime.name();
    private static final String DESC = "DESC";

    private static final HdfsFileFilter avroFileFilter = new HdfsFileFilter() {
        @Override
        public boolean accept(FileStatus file) {
            return file.getPath().getName().endsWith("avro");
        }
    };

    @Inject
    private Configuration yarnConfiguration;

    @Test(groups = "functional")
    public void testGenerateLaunchUniverseJobContactLimit() throws Exception {
        GenerateLaunchUniverseJobConfig config = new GenerateLaunchUniverseJobConfig();
        config.setWorkspace("testGenerateLaunchUniverseJobContactLimit");

        config.setMaxContactsPerAccount(2L);
        config.setMaxEntitiesToLaunch(20L);
        config.setContactsPerAccountSortAttribute(CDL_UPDATED_TIME);
        config.setContactsPerAccountSortDirection(DESC);

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLaunchUniverseJob.class, config);
        log.info("TestGenerateLaunchUniverseJobContactLimit Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 16);
    }

    @Test(groups = "functional")
    public void testGenerateLaunchUniverseJobNoLimit() throws Exception {
        GenerateLaunchUniverseJobConfig config = new GenerateLaunchUniverseJobConfig();
        config.setWorkspace("testGenerateLaunchUniverseJobNoLimit");

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLaunchUniverseJob.class, config);
        log.info("TestGenerateLaunchUniverseJobNoLimit Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 20);
    }

    @Test(groups = "functional")
    public void testGenerateLaunchUniverseJobBothLimits() throws Exception {
        GenerateLaunchUniverseJobConfig config = new GenerateLaunchUniverseJobConfig();
        config.setWorkspace("testGenerateLaunchUniverseJobBothLimits");

        config.setMaxContactsPerAccount(2L);
        config.setMaxEntitiesToLaunch(13L);
        config.setContactsPerAccountSortAttribute(CDL_UPDATED_TIME);
        config.setContactsPerAccountSortDirection(DESC);

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLaunchUniverseJob.class, config);
        log.info("TestGenerateLaunchUniverseJobBothLimits Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 13);
    }

    @Test(groups = "functional")
    public void testGenerateLaunchUniverseJobColumnNotFound() throws Exception {
        // When sort column is not found, use ContactId to sort
        GenerateLaunchUniverseJobConfig config = new GenerateLaunchUniverseJobConfig();
        config.setWorkspace("testGenerateLaunchUniverseJobColumnNotFound");

        config.setMaxContactsPerAccount(2L);
        config.setContactsPerAccountSortAttribute("Unknown Column");
        config.setContactsPerAccountSortDirection(DESC);

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLaunchUniverseJob.class, config);
        log.info("TestGenerateLaunchUniverseJobColumnNotFound Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 16);
    }

    @Test(groups = "functional")
    public void testGenerateLaunchUniverseJobDefaultSort() throws Exception {
        // When sort column is not found, use ContactId to sort
        GenerateLaunchUniverseJobConfig config = new GenerateLaunchUniverseJobConfig();
        config.setWorkspace("testGenerateLaunchUniverseJobDefaultSort");
        config.setMaxContactsPerAccount(2L);

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLaunchUniverseJob.class, config);
        log.info("TestGenerateLaunchUniverseJobDefaultSort Results: " + JsonUtils.serialize(result));

        Assert.assertTrue(verifyDefaultSort(result));
    }

    private boolean verifyDefaultSort(SparkJobResult result) throws Exception {
        String hdfsDir = result.getTargets().get(0).getPath();
        String fieldName = "ContactId";
        List<String> avroFilePaths = HdfsUtils.getFilesForDir(yarnConfiguration, hdfsDir, avroFileFilter);

        for (Object filePath : avroFilePaths) {
            String filePathStr = filePath.toString();
            log.info("File path is: " + filePathStr);
    
            try (FileReader<GenericRecord> reader = AvroUtils.getAvroFileReader(yarnConfiguration, new Path(filePathStr))) {
                for (GenericRecord record : reader) {
                    String contactId = getString(record, fieldName);
                    List<String> expectedIds = getExpectedIds();
                    if (!expectedIds.contains(contactId)) {
                        log.info("Unexpected contactId: " + contactId);
                        return false;
                    }
                }
            }
        }

        return true;
    }

    private static String getString(GenericRecord record, String field) throws Exception {
        String value;
        try {
            value = record.get(field).toString();
        } catch (Exception e) {
            value = "";
        }
        return value;
    }

    private static List<String> getExpectedIds() {
        List<String> expectedIds = Arrays.asList(
                "C11", "C12", "C21", "C22", "C3", "C4", "C5", "C6",
                "C71", "C72", "C81", "C82", "C91", "C92", "C110", "C115");
        return expectedIds;
    }
}
