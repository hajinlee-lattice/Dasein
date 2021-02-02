package com.latticeengines.spark.exposed.job.cdl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    @Test(groups = "functional")
    public void testGenerateLaunchUniverseJobThresholdLimitNotApplied() throws Exception {
        GenerateLaunchUniverseJobConfig config = new GenerateLaunchUniverseJobConfig();
        config.setWorkspace("testGenerateLaunchUniverseJobThresholdLimitNotApplied");

        config.setContactAccountRatioThreshold(2L);
        config.setMaxContactsPerAccount(2L);
        config.setContactsPerAccountSortAttribute(CDL_UPDATED_TIME);
        config.setContactsPerAccountSortDirection(DESC);

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLaunchUniverseJob.class, config);
        log.info("TestGenerateLaunchUniverseJobThresholdLimitNotApplied Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 16);
    }

    @Test(groups = "functional")
    public void testGenerateLaunchUniverseJobThresholdLimitApplied() throws Exception {
        GenerateLaunchUniverseJobConfig config = new GenerateLaunchUniverseJobConfig();
        config.setWorkspace("testGenerateLaunchUniverseJobThresholdLimitApplied");
        config.setContactAccountRatioThreshold(2L);

        log.info("Config: " + JsonUtils.serialize(config));
        String error = null;

        try {
            SparkJobResult result = runSparkJob(GenerateLaunchUniverseJob.class, config);
            log.info("TestGenerateLaunchUniverseJobThresholdLimitApplied Results: " + JsonUtils.serialize(result));
        } catch (Exception e) {
            error = e.getMessage();
        }

        Assert.assertNotNull(error);
    }

    @Test(groups = "functional")
    public void testGenerateLaunchUniverseJobThresholdLimitApplied2() throws Exception {
        GenerateLaunchUniverseJobConfig config = new GenerateLaunchUniverseJobConfig();
        config.setWorkspace("testGenerateLaunchUniverseJobThresholdLimitApplied2");
        config.setContactAccountRatioThreshold(2L);
        config.setMaxContactsPerAccount(3L);

        log.info("Config: " + JsonUtils.serialize(config));
        String error = null;

        try {
            SparkJobResult result = runSparkJob(GenerateLaunchUniverseJob.class, config);
            log.info("TestGenerateLaunchUniverseJobThresholdLimitApplied2 Results: " + JsonUtils.serialize(result));
        } catch (Exception e) {
            error = e.getMessage();
        }

        Assert.assertNotNull(error);
    }

    private boolean verifyDefaultSort(SparkJobResult result) throws Exception {
        String hdfsDir = result.getTargets().get(0).getPath();
        String fieldName = "ContactId";
        String ACCOUNT_ID = "AccountId";
        List<String> avroFilePaths = HdfsUtils.getFilesForDir(yarnConfiguration, hdfsDir, avroFileFilter);
        Map<String, List<String>> expectedIdsMap = getExpectedIdsMap();
        int index = 0;

        for (Object filePath : avroFilePaths) {
            String filePathStr = filePath.toString();
            log.info("File path is: " + filePathStr);
    
            try (FileReader<GenericRecord> reader = AvroUtils.getAvroFileReader(yarnConfiguration, new Path(filePathStr))) {
                for (GenericRecord record : reader) {
                    String contactId = getString(record, fieldName);
                    String accountId = getString(record, ACCOUNT_ID);
                    List<String> expectedIds = expectedIdsMap.get(accountId);
                    log.info("contactId: " + contactId + " / accountId: " + accountId);
                    if (!expectedIds.get(index).equals(contactId)) {
                        log.info("Unexpected contactId at index: " + index);
                        return false;
                    }
                    if (index == expectedIds.size() - 1) {
                        // Sometimes two accounts are combined in one avro file
                        index = 0;
                    } else {
                        index++;
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

    private static Map<String, List<String>> getExpectedIdsMap() {
        Map<String, List<String>> idMap = new HashMap<>();
        List<String> account1 = Arrays.asList("C11", "C12");
        List<String> account2 = Arrays.asList("C21", "C22");
        List<String> account3 = Arrays.asList("C3");
        List<String> account4 = Arrays.asList("C4");
        List<String> account5 = Arrays.asList("C5");
        List<String> account6 = Arrays.asList("C6");
        List<String> account7 = Arrays.asList("C71", "C72");
        List<String> account8 = Arrays.asList("C81", "C82");
        List<String> account9 = Arrays.asList("C91", "C92");
        List<String> account10 = Arrays.asList("C110", "C115");

        idMap.put("A1", account1);
        idMap.put("A2", account2);
        idMap.put("A3", account3);
        idMap.put("A4", account4);
        idMap.put("A5", account5);
        idMap.put("A6", account6);
        idMap.put("A7", account7);
        idMap.put("A8", account8);
        idMap.put("A9", account9);
        idMap.put("A10", account10);

        return idMap;
    }

    @Test(groups = "functional")
    public void testGenerateLaunchUniverseJobContactAccountRatioExceed() throws Exception {
        GenerateLaunchUniverseJobConfig config = new GenerateLaunchUniverseJobConfig();
        config.setWorkspace("testGenerateLaunchUniverseJobBothLimits");

        config.setContactAccountRatioThreshold(3L);
        config.setContactsPerAccountSortAttribute(CDL_UPDATED_TIME);
        config.setContactsPerAccountSortDirection(DESC);

        log.info("Config: " + JsonUtils.serialize(config));
        try {
            SparkJobResult result = runSparkJob(GenerateLaunchUniverseJob.class, config);
        } catch (RuntimeException e){
            return;
        }
        Assert.fail("Failed in testGenerateLaunchUniverseJobContactAccountRatioExceed\n");
    }

}
