package com.latticeengines.apps.cdl.end2end.dataingestion;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CustomEventModelingConfig;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.testframework.exposed.proxy.pls.ModelSummaryProxy;

public class CustomEventModelEnd2EndDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CustomEventModelEnd2EndDeploymentTestNG.class);
    private static final boolean USE_EXISTING_TENANT = true;
    private static final String EXISTING_TENANT = "JLM1521680290015";

    private MetadataSegment testSegment;
    private RatingEngine testRatingEngine;
    private AIModel testAIModel;
    private SourceFile testSourceFile;
    private final String testSourceFileName = "CustomEventModelE2ETestFile.csv";

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private SegmentProxy segmentProxy;

    private final String targetProductId = "A74D1222394534E6B450CA006C20D48D";
    private final String trainingProductId = "A80D4770376C1226C47617C071324C0B";

    @BeforeClass(groups = { "end2end" })
    public void setup() throws Exception {
        if (USE_EXISTING_TENANT) {
            testBed.useExistingTenantAsMain(EXISTING_TENANT);
            testBed.switchToSuperAdmin();
            mainTestTenant = testBed.getMainTestTenant();
        } else {
            super.setup();
            resumeVdbCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);
        }
        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
        attachProtectedProxy(modelSummaryProxy);
        attachProtectedProxy(fileUploadProxy);
        setupTestRatingEngine();
    }

    @Test(groups = "end2end")
    public void runTest() {
        log.info("Start modeling ...");
        String modelingWorkflowApplicationId = ratingEngineProxy.modelRatingEngine(mainTestTenant.getId(),
                testRatingEngine.getId(), testAIModel.getId(), "bnguyen@lattice-engines.com");
        log.info(String.format("Workflow application id is %s", modelingWorkflowApplicationId));
        testRatingEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), testRatingEngine.getId());
        JobStatus completedStatus = waitForWorkflowStatus(modelingWorkflowApplicationId, false);
        Assert.assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    @AfterClass(groups = { "end2end" })
    public void cleanup() {
        ratingEngineProxy.deleteRatingEngine(mainTestTenant.getId(), testRatingEngine.getId());
        segmentProxy.deleteSegmentByName(mainTestTenant.getId(), testSegment.getName());

    }

    private void setupTestSegment() {
        testSegment = constructTargetSegment();
        testSegment = segmentProxy.createOrUpdateSegment(mainTestTenant.getId(), testSegment);
    }

    private void setupSourceFile() {
        Resource csvResrouce = new ClassPathResource("end2end/csv/CustomEventModelTest.csv",
                Thread.currentThread().getContextClassLoader());
        testSourceFile = fileUploadProxy.uploadFile(testSourceFileName, false, "CustomEventModelTest.csv",
                SchemaInterpretation.Account, "Account", csvResrouce);
        FieldMappingDocument fmDoc = fileUploadProxy.getFieldMappings(testSourceFileName, "Account");
        for (FieldMapping fm : fmDoc.getFieldMappings()) {
            if (fm.getUserField().equals("Event")) {
                fm.setMappedField("Event");
                fm.setMappedToLatticeField(true);
            }
            if (fm.getUserField().equals("name")) {
                fm.setMappedField("CompanyName");
                fm.setMappedToLatticeField(true);
            }
            if (fm.getUserField().equals("ID")) {
                fm.setMappedField("ID");
                fm.setMappedToLatticeField(true);
            }
        }

        fileUploadProxy.saveFieldMappingDocument(testSourceFileName, fmDoc);
    }

    private void setupTestRatingEngine() {
        setupTestSegment();
        setupSourceFile();

        testRatingEngine = new RatingEngine();
        testRatingEngine.setDisplayName("CreateAIModelDeploymentTestRating");
        testRatingEngine.setTenant(mainTestTenant);
        testRatingEngine.setType(RatingEngineType.CUSTOM_EVENT);
        testRatingEngine.setSegment(testSegment);
        testRatingEngine.setCreatedBy("bnguyen@lattice-engines.com");
        testRatingEngine.setCreated(new Date());
        testRatingEngine.setCreated(new Date());

        testRatingEngine = ratingEngineProxy.createOrUpdateRatingEngine(mainTestTenant.getId(), testRatingEngine);
        testAIModel = (AIModel) testRatingEngine.getActiveModel();
        CustomEventModelingConfig advancedConf = CustomEventModelingConfig.getAdvancedModelingConfig(testAIModel);
        advancedConf.setDataStores(
                Arrays.asList(CustomEventModelingConfig.DataStore.CDL, CustomEventModelingConfig.DataStore.DataCloud));
        advancedConf.setCustomEventModelingType(CustomEventModelingType.CDL);
        advancedConf.setDeduplicationType(DedupType.ONELEADPERDOMAIN);
        advancedConf.setExcludePublicDomains(false);
        advancedConf.setSourceFileName(testSourceFile.getName());

        testAIModel = (AIModel) ratingEngineProxy.updateRatingModel(mainTestTenant.getId(), testRatingEngine.getId(),
                testAIModel.getId(), testAIModel);
    }

}
