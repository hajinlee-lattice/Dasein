package com.latticeengines.cdl.workflow.steps;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.util.UUID;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.amazonaws.services.sns.model.PublishResult;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.cdl.workflow.steps.play.DeltaCampaignLaunchExportPublishToSNSStep;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.LaunchBaseType;
import com.latticeengines.domain.exposed.pls.ExternalSystemAuthentication;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
import com.latticeengines.domain.exposed.pls.cdl.channel.OutreachChannelConfig;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.DeltaCampaignLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.DeltaCampaignLaunchExportPublishToSNSConfiguration;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.proxy.exposed.cdl.LookupIdMappingProxy;
import com.latticeengines.workflow.functionalframework.WorkflowTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-cdl-workflow-context.xml",
        "classpath:test-serviceflows-cdl-context.xml" })
public class DeltaCampaignLaunchExportPublishToSNSStepTestNG extends WorkflowTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DeltaCampaignLaunchExportPublishToSNSStepTestNG.class);
    private static final CustomerSpace CDL_WF_TEST_CUSTOMERSPACE = CustomerSpace.parse("CDLWFTests.CDLWFTests.CDLWFTests");

    @Mock
    private DropBoxProxy dropboxProxy;

    @Mock
    private LookupIdMappingProxy lookupIdMappingProxy;

    @Inject
    private DeltaCampaignLaunchExportPublishToSNSStep publishToSNSStep;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    protected Configuration yarnConfiguration;

    private Tenant tenant;
    private LookupIdMap lookupIdMap;
    private DropBoxSummary dropbox;
    private DeltaCampaignLaunchExportPublishToSNSConfiguration publishConfig;

    private String solutionInstanceId = UUID.randomUUID().toString();
    private String audienceId = UUID.randomUUID().toString();

    @Override
    @BeforeClass(groups = "manual")
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        setupTenant();
        setupDropbox();
        setupStep();
        setupLookupIdMap();

        publishConfig = new DeltaCampaignLaunchExportPublishToSNSConfiguration();
        publishConfig.setExternalAudienceId(audienceId);
        publishConfig.setExternalAudienceName("externalAudienceName");
        publishConfig.setLookupIdMap(lookupIdMap);
    }

    @Test(groups = "manual")
    public void testOutreachSequenceSnsTopic() throws Exception {
        OutreachChannelConfig outreachConfig = new OutreachChannelConfig();
        outreachConfig.setLaunchBaseType(LaunchBaseType.AUDIENCE);

        publishConfig.setChannelConfig(outreachConfig);
        publishToSNSStep.setConfiguration(publishConfig);

        String workflowRequestId = UUID.randomUUID().toString();
        publishToSNSStep.putObjectInContext(DeltaCampaignLaunchWorkflowConfiguration.RECOMMENDATION_WORKFLOW_REQUEST_ID,
                workflowRequestId);

        String wfRequestId = publishToSNSStep
                .getObjectFromContext(DeltaCampaignLaunchWorkflowConfiguration.RECOMMENDATION_WORKFLOW_REQUEST_ID, String.class);

        PublishResult publishResult = publishToSNSStep.publishToSnsTopic(CDL_WF_TEST_CUSTOMERSPACE, wfRequestId);
        log.info(JsonUtils.serialize(publishResult));
        Assert.assertNotNull(publishResult);
    }

    @Test(groups = "manual")
    public void testOutreachTaskSnsTopic() throws Exception {
        OutreachChannelConfig outreachConfig = new OutreachChannelConfig();
        outreachConfig.setLaunchBaseType(LaunchBaseType.TASK);
        outreachConfig.setAudienceType(AudienceType.CONTACTS);
        outreachConfig.setTaskType("Meet in Person");
        outreachConfig.setTaskPriority("Urgent");
        outreachConfig.setTaskDescription("This task needs to be done ASAP!");

        publishConfig.setChannelConfig(outreachConfig);
        publishToSNSStep.setConfiguration(publishConfig);

        String workflowRequestId = UUID.randomUUID().toString();
        publishToSNSStep.putObjectInContext(DeltaCampaignLaunchWorkflowConfiguration.RECOMMENDATION_WORKFLOW_REQUEST_ID,
                workflowRequestId);

        String wfRequestId = publishToSNSStep
                .getObjectFromContext(DeltaCampaignLaunchWorkflowConfiguration.RECOMMENDATION_WORKFLOW_REQUEST_ID, String.class);

        PublishResult publishResult = publishToSNSStep.publishToSnsTopic(CDL_WF_TEST_CUSTOMERSPACE, wfRequestId);
        log.info(JsonUtils.serialize(publishResult));
        Assert.assertNotNull(publishResult);
    }

    private void setupTenant() throws Exception {
        tenant = tenantEntityMgr.findByTenantId(CDL_WF_TEST_CUSTOMERSPACE.toString());
        if (tenant != null) {
            tenantEntityMgr.delete(tenant);
        }
        tenant = new Tenant();
        tenant.setId(CDL_WF_TEST_CUSTOMERSPACE.toString());
        tenant.setName(CDL_WF_TEST_CUSTOMERSPACE.toString());
        tenantEntityMgr.create(tenant);
        MultiTenantContext.setTenant(tenant);

        com.latticeengines.domain.exposed.camille.Path path = //
                PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), CDL_WF_TEST_CUSTOMERSPACE);
        HdfsUtils.rmdir(yarnConfiguration, path.toString());
        HdfsUtils.mkdir(yarnConfiguration, path.toString());
    }

    private void setupDropbox() throws Exception {
        dropbox = new DropBoxSummary();
        dropbox.setDropBox(UUID.randomUUID().toString());
        when(dropboxProxy.getDropBox(anyString())).thenReturn(dropbox);
    }

    private void setupStep() throws Exception {
        publishToSNSStep.setExecutionContext(new ExecutionContext());
        publishToSNSStep.setDropBoxProxy(dropboxProxy);
    }

    private void setupLookupIdMap() throws Exception {
        lookupIdMap = new LookupIdMap();
        ExternalSystemAuthentication extSysAuth = new ExternalSystemAuthentication();
        extSysAuth.setSolutionInstanceId(solutionInstanceId);
        lookupIdMap.setExternalAuthentication(extSysAuth);
        lookupIdMap.setExternalSystemName(CDLExternalSystemName.Outreach);
        lookupIdMap.setExternalSystemType(CDLExternalSystemType.MAP);
    }

}
