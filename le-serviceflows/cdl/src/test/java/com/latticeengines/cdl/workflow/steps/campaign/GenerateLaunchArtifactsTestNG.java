package com.latticeengines.cdl.workflow.steps.campaign;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
import com.latticeengines.workflow.functionalframework.WorkflowTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-cdl-workflow-context.xml",
        "classpath:test-serviceflows-cdl-context.xml" })
public class GenerateLaunchArtifactsTestNG extends WorkflowTestNGBase {

    @Inject
    private GenerateLaunchArtifacts generateLaunchArtifacts;

    @Test(groups = "functional")
    public void testShouldSkipStep() {
        generateLaunchArtifacts.setExecutionContext(new ExecutionContext());

        Map<String, Long> counts = new HashMap<>();
        counts.put("FULL_CONTACTS_UNIVERSE", 81541L);
        counts.put("ADDED_CONTACTS_DELTA_TABLE", 81541L);
        AudienceType audienceType = AudienceType.CONTACTS;
        CDLExternalSystemName channelType = CDLExternalSystemName.AWS_S3;
        Assert.assertFalse(invokeShouldSkipStep(audienceType, channelType, counts));

        counts = new HashMap<>();
        counts.put("FULL_ACCOUNTS_UNIVERSE", 81541L);
        audienceType = AudienceType.ACCOUNTS;
        channelType = CDLExternalSystemName.Salesforce;
        Assert.assertTrue(invokeShouldSkipStep(audienceType, channelType, counts));

        counts = null;
        audienceType = AudienceType.ACCOUNTS;
        channelType = CDLExternalSystemName.Salesforce;
        Assert.assertTrue(invokeShouldSkipStep(audienceType, channelType, counts));

        audienceType = AudienceType.CONTACTS;
        channelType = CDLExternalSystemName.Marketo;
        Assert.assertTrue(invokeShouldSkipStep(audienceType, channelType, counts));

        audienceType = AudienceType.CONTACTS;
        channelType = CDLExternalSystemName.Marketo;
        counts = new HashMap<>();
        counts.put("FULL_CONTACTS_UNIVERSE", 81541L);
        counts.put("REMOVED_CONTACTS_DELTA_TABLE", 81541L);
        Assert.assertFalse(invokeShouldSkipStep(audienceType, channelType, counts));
    }

    private boolean invokeShouldSkipStep(AudienceType audienceType, CDLExternalSystemName channelType,
            Map<String, Long> counts) {
        generateLaunchArtifacts.putObjectInContext("DELTA_TABLE_COUNTS", counts);
        return generateLaunchArtifacts.shouldSkipStep(audienceType, channelType);
    }
}
