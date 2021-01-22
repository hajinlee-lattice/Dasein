package com.latticeengines.cdl.workflow.steps;

import javax.inject.Inject;

import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.cdl.workflow.steps.play.DeltaCampaignLaunchExportFileGeneratorStep;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.LaunchBaseType;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
import com.latticeengines.domain.exposed.pls.cdl.channel.OutreachChannelConfig;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.DeltaCampaignLaunchExportFilesGeneratorConfiguration;
import com.latticeengines.workflow.functionalframework.WorkflowTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-cdl-workflow-context.xml",
        "classpath:test-serviceflows-cdl-context.xml" })
public class DeltaCampaignLaunchExportFileGeneratorStepTestNG extends WorkflowTestNGBase {

    @Inject
    private DeltaCampaignLaunchExportFileGeneratorStep fileGeneratorStep;

    private DeltaCampaignLaunchExportFilesGeneratorConfiguration config;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        //
    }

    @Test(groups = "functional")
    public void testShouldIgnoreAccountsWithoutContacts() throws Exception {
        OutreachChannelConfig outreachConfig = new OutreachChannelConfig();
        outreachConfig.setLaunchBaseType(LaunchBaseType.AUDIENCE);
        outreachConfig.setAudienceType(AudienceType.CONTACTS);

        DeltaCampaignLaunchExportFilesGeneratorConfiguration config = new DeltaCampaignLaunchExportFilesGeneratorConfiguration();
        config.setChannelConfig(outreachConfig);
        config.setDestinationSysName(CDLExternalSystemName.Outreach);
        config.setDestinationSysType(CDLExternalSystemType.MAP);

        boolean result = fileGeneratorStep.shouldIgnoreAccountsWithoutContacts(config);
        Assert.assertTrue(result);
    }

    @Test(groups = "functional")
    public void testShouldNotIgnoreAccountsWithoutContacts() throws Exception {
        OutreachChannelConfig outreachConfig = new OutreachChannelConfig();
        outreachConfig.setLaunchBaseType(LaunchBaseType.TASK);
        outreachConfig.setAudienceType(AudienceType.ACCOUNTS);

        DeltaCampaignLaunchExportFilesGeneratorConfiguration config = new DeltaCampaignLaunchExportFilesGeneratorConfiguration();
        config.setChannelConfig(outreachConfig);
        config.setDestinationSysName(CDLExternalSystemName.Outreach);
        config.setDestinationSysType(CDLExternalSystemType.MAP);

        boolean result = fileGeneratorStep.shouldIgnoreAccountsWithoutContacts(config);
        Assert.assertFalse(result);
    }

}
