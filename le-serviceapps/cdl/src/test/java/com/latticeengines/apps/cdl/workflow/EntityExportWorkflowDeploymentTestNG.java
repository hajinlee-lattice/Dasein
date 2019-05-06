package com.latticeengines.apps.cdl.workflow;

import java.lang.reflect.Method;
import java.util.Collections;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ReflectionUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.end2end.CDLEnd2EndDeploymentTestNGBase;
import com.latticeengines.apps.cdl.end2end.UpdateTransactionDeploymentTestNG;
import com.latticeengines.apps.cdl.service.AtlasExportService;
import com.latticeengines.apps.cdl.testframework.CDLWorkflowFrameworkDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.cdl.EntityExportRequest;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.serviceflows.cdl.EntityExportWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.export.EntityExportStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;

/**
 * dpltc deploy -a admin,pls,lp,cdl,metadata,matchapi,workflowapi
 */
public class EntityExportWorkflowDeploymentTestNG extends CDLWorkflowFrameworkDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(EntityExportWorkflowDeploymentTestNG.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private EntityExportWorkflowSubmitter entityExportWorkflowSubmitter;

    @Inject
    private AtlasExportService atlasExportService;

    @BeforeClass(groups = "manual" )
    public void setup() throws Exception {
        boolean useExistingTenant = true;
        if (useExistingTenant) {
            testBed.useExistingTenantAsMain("LETest1557130991019");
            testBed.switchToSuperAdmin();
            mainTestTenant = testBed.getMainTestTenant();
            mainTestCustomerSpace = CustomerSpace.parse(mainTestTenant.getId());
        } else {
            setupTestEnvironment();
            checkpointService.resumeCheckpoint( //
                    UpdateTransactionDeploymentTestNG.CHECK_POINT, //
                    CDLEnd2EndDeploymentTestNGBase.S3_CHECKPOINTS_VERSION);
        }
        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
    }

    @Test(groups = "manual")
    public void testWorkflow() throws Exception {
        DataCollection.Version version = dataCollectionProxy.getActiveVersion(mainTestTenant.getId());
        EntityExportRequest request = new EntityExportRequest();
        request.setDataCollectionVersion(version);
        AtlasExport atlasExport = atlasExportService.createAtlasExport(mainTestCustomerSpace.toString(),
                AtlasExportType.ACCOUNT_AND_CONTACT);
        Method method = ReflectionUtils.findMethod(EntityExportWorkflowSubmitter.class,
                "configure", String.class, EntityExportRequest.class, AtlasExport.class);
        Assert.assertNotNull(method);
        ReflectionUtils.makeAccessible(method);
        EntityExportWorkflowConfiguration configuration = (EntityExportWorkflowConfiguration) //
                ReflectionUtils.invokeMethod(method, entityExportWorkflowSubmitter, //
                        mainTestCustomerSpace.toString(), request, atlasExport);
        Assert.assertNotNull(configuration);

        EntityExportStepConfiguration stepConfiguration = JsonUtils.deserialize( //
                configuration.getStepConfigRegistry().get(EntityExportStepConfiguration.class.getSimpleName()), //
                EntityExportStepConfiguration.class);
        stepConfiguration.setSaveToLocal(true);
        configuration.add(stepConfiguration);

        runWorkflow(generateWorkflowTestConfiguration(null, //
                "entityExportWorkflow", configuration, null));

        verifyTest();
    }

    @Override
    protected void verifyTest() {
    }

}
