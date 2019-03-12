package com.latticeengines.cdl.workflow.steps;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.UUID;

import javax.inject.Inject;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.amazonaws.services.sns.model.PublishResult;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.pls.ExternalSystemAuthentication;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.PlayLaunchExportFilesToS3Configuration;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.proxy.exposed.cdl.LookupIdMappingProxy;
import com.latticeengines.serviceflows.workflow.export.PlayLaunchExportFilesToS3Step;
import com.latticeengines.workflow.functionalframework.WorkflowTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-cdl-workflow-context.xml",
        "classpath:test-serviceflows-cdl-context.xml" })
public class PublishSnsMessageFunctionalTestNG extends WorkflowTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PublishSnsMessageFunctionalTestNG.class);

    @Mock
    private DropBoxProxy dropboxProxy;

    @Mock
    private LookupIdMappingProxy lookupIdMappingProxy;

    @Inject
    private PlayLaunchExportFilesToS3Step exportStep;

    private String customerSpace = "CUSTOMER_SPACE";

    private String workflowRequestId = UUID.randomUUID().toString();

    private String audienceId = UUID.randomUUID().toString();

    @Override
    @BeforeClass(groups = "manual")
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        exportStep.setDropBoxProxy(dropboxProxy);
        exportStep.setS3ExportFiles(Arrays.asList("lattice-engines-test/dropfolder/example.csv",
                "lattice-engines-test/dropfolder/example.json"));

        PlayLaunchExportFilesToS3Configuration config = new PlayLaunchExportFilesToS3Configuration();
        LookupIdMap lookupIdMap = new LookupIdMap();
        ExternalSystemAuthentication extSysAuth = new ExternalSystemAuthentication();
        extSysAuth.setSolutionInstanceId(UUID.randomUUID().toString());
        lookupIdMap.setExternalAuthentication(extSysAuth);
        lookupIdMap.setExternalSystemName(CDLExternalSystemName.Marketo);

        config.setLookupIdMap(lookupIdMap);
        config.setExternalAudienceId(audienceId);
        config.setExternalAudienceName("externalAudienceName");

        exportStep.setConfiguration(config);
    }

    @Test(groups = "manual")
    public void testPublishToSnsTopic() {
        DropBoxSummary dropbox = new DropBoxSummary();
        dropbox.setDropBox(UUID.randomUUID().toString());
        when(dropboxProxy.getDropBox(anyString())).thenReturn(dropbox);

        PublishResult publishResult = exportStep.publishToSnsTopic(customerSpace, workflowRequestId);
        log.info(JsonUtils.serialize(publishResult));
        Assert.assertNotNull(publishResult);
    }
}
