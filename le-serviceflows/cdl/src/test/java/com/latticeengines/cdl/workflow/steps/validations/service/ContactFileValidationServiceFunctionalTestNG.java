package com.latticeengines.cdl.workflow.steps.validations.service;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.cdl.workflow.CDLWorkflowFunctionalTestNGBase;
import com.latticeengines.cdl.workflow.steps.validations.service.impl.ContactFileValidationService;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.EntityValidationSummary;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl.ContactFileValidationConfiguration;

public class ContactFileValidationServiceFunctionalTestNG extends CDLWorkflowFunctionalTestNGBase {

    @Inject
    private ContactFileValidationService contactFileValidationService;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        super.setup();
        CustomerSpace customerSpace = CustomerSpace.parse(tenant.getId());
        String hdfsDir = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString();
        fileDestination = hdfsDir + "/contact/";
        if (!HdfsUtils.fileExists(yarnConfiguration, fileDestination)) {
            HdfsUtils.mkdir(yarnConfiguration, fileDestination);
        }
        fileName = "contact.avro";
        InputStream in = testArtifactService.readTestArtifactAsStream(TEST_AVRO_DIR, TEST_AVRO_VERSION, "Contact1" +
                ".avro");
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, in, fileDestination + fileName);
    }

    @AfterClass(groups = {"functional"})
    public void teardown() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, fileDestination);
    }
    @Override
    protected String getFlowBeanName() {
        return null;
    }

    @Test(groups = "functional")
    public void testContactFileValidations() {
        // legacy tenant
        ContactFileValidationConfiguration configuration = new ContactFileValidationConfiguration();
        configuration.setEntity(BusinessEntity.Contact);
        configuration.setEnableEntityMatch(false);
        configuration.setEnableEntityMatchGA(false);
        configuration.setPathList(Collections.singletonList(fileDestination + fileName));

        List<String> processedRecords = Collections.singletonList("50");
        EntityValidationSummary summary = contactFileValidationService.validate(configuration, processedRecords);
        Assert.assertNotNull(summary);
        Assert.assertEquals(summary.getErrorLineNumber(), 0L);

        // entity match GA
        processedRecords = Arrays.asList("50");
        configuration.setEnableEntityMatch(false);
        configuration.setEnableEntityMatchGA(true);
        summary = contactFileValidationService.validate(configuration, processedRecords);
        Assert.assertEquals(50L, summary.getErrorLineNumber());
        Assert.assertEquals(processedRecords.get(0), "0");

        // entity match
        processedRecords = Collections.singletonList("50");
        configuration.setEnableEntityMatch(true);
        configuration.setEnableEntityMatchGA(false);
        summary = contactFileValidationService.validate(configuration, processedRecords);
        Assert.assertEquals(0L, summary.getErrorLineNumber());

    }
}
