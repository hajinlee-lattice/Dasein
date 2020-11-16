package com.latticeengines.spark.exposed.job.cdl;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

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

}
