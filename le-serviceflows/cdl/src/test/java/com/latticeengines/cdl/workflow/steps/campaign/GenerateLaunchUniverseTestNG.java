package com.latticeengines.cdl.workflow.steps.campaign;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.spark.cdl.GenerateLaunchUniverseJobConfig;
import com.latticeengines.workflow.functionalframework.WorkflowTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-cdl-workflow-context.xml",
        "classpath:test-serviceflows-cdl-context.xml" })
public class GenerateLaunchUniverseTestNG extends WorkflowTestNGBase {

    @Inject
    private GenerateLaunchUniverse generateLaunchUniverse;

    @Inject
    private Configuration yarnConfiguration;

    @Test(groups = "functional")
    public void testUseSparkJobContactsPerAccount() {
        GenerateLaunchUniverseJobConfig config = new GenerateLaunchUniverseJobConfig();

        config.setMaxContactsPerAccount(2L);
        config.setMaxEntitiesToLaunch(20L);
        boolean useSparkJobContactsPerAccount = generateLaunchUniverse.useSparkJobContactsPerAccount( //
            BusinessEntity.Contact, config.getMaxContactsPerAccount(), config.getContactAccountRatioThreshold());
        Assert.assertTrue(useSparkJobContactsPerAccount);
    }

    @Test(groups = "functional")
    public void testUseContactPerAccountLimit() {
        GenerateLaunchUniverseJobConfig config = new GenerateLaunchUniverseJobConfig();

        config.setMaxEntitiesToLaunch(20L);
        config.setContactAccountRatioThreshold(100L);
        boolean useContactPerAccountLimit = generateLaunchUniverse.useContactPerAccountLimit( //
            BusinessEntity.Contact, config.getMaxContactsPerAccount());
        Assert.assertFalse(useContactPerAccountLimit);
    }

}
