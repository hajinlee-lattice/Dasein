package com.latticeengines.apps.cdl.end2end;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CustomEventModelingConfig;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.lp.BucketedScoreProxy;
import com.latticeengines.testframework.exposed.proxy.pls.ModelSummaryProxy;

public class CustomEventModelEnd2EndDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CustomEventModelEnd2EndDeploymentTestNG.class);
    private static final boolean USE_EXISTING_TENANT = false;
    private static final String EXISTING_TENANT = "LETest1528844192916";

    private MetadataSegment testSegment;
    private RatingEngine lpiCERatingEngine;
    private RatingEngine cdlCERatingEngine;
    private AIModel lpiCEAIModel;
    private AIModel cdlCEAIModel;
    private SourceFile testSourceFile;
    private final String testSourceFileName = "CustomEventModelE2ETestFile.csv";

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private BucketedScoreProxy bucketedScoreProxy;

    @Inject
    private SegmentProxy segmentProxy;

    @BeforeClass(groups = { "end2end", "manual", "precheckin" })
    public void setup() {
    }

    /**
     * This test is part of CD pipeline and Trunk Health
     */
    @Test(groups = { "end2end", "precheckin" })
    public void end2endCDLStyleCustomEventModelTest() throws Exception {
        setupEnd2EndTestEnvironment();
        resumeCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);
        CustomEventModelingType testType = CustomEventModelingType.CDL;
        bootstrap(testType);
        runCustomEventModel(testType);
    }

    /**
     * This test is part of CD pipeline
     */
    // @Test(groups = "end2end")
    public void end2endLPIStyleCustomEventModelTest() throws Exception {
        setupEnd2EndTestEnvironment();
        resumeCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);
        CustomEventModelingType testType = CustomEventModelingType.LPI;
        bootstrap(testType);
        runCustomEventModel(testType);
    }

    /**
     * This test is for generating model artifacts for other tests
     */
    @Test(groups = "manual")
    public void manualTest() throws Exception {
        if (USE_EXISTING_TENANT) {
            testBed.useExistingTenantAsMain(EXISTING_TENANT);
            testBed.switchToSuperAdmin();
            mainTestTenant = testBed.getMainTestTenant();
        } else {
            setupEnd2EndTestEnvironment();
            resumeCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);
        }
        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
        CustomEventModelingType testType = CustomEventModelingType.CDL;
        bootstrap(testType);
        runCustomEventModel(testType);
    }

    private void runCustomEventModel(CustomEventModelingType type) {
        log.info("Starting Custom Event modeling ...");
        RatingEngine testRatingEngine = type == CustomEventModelingType.CDL ? cdlCERatingEngine : lpiCERatingEngine;
        RatingModel testAIModel = type == CustomEventModelingType.CDL ? cdlCEAIModel : lpiCEAIModel;
        verifyBucketMetadataNotGenerated(testRatingEngine);
        String modelingWorkflowApplicationId = ratingEngineProxy.modelRatingEngine(mainTestTenant.getId(),
                testRatingEngine.getId(), testAIModel.getId(), "bnguyen@lattice-engines.com");
        log.info(String.format("Workflow application id is %s", modelingWorkflowApplicationId));
        testRatingEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), testRatingEngine.getId());
        JobStatus completedStatus = waitForWorkflowStatus(modelingWorkflowApplicationId, false);
        Assert.assertEquals(completedStatus, JobStatus.COMPLETED);
        verifyBucketMetadataGenerated(testRatingEngine);
        Assert.assertEquals(
                ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), testRatingEngine.getId()).getStatus(),
                RatingEngineStatus.INACTIVE);
    }

    private void bootstrap(CustomEventModelingType type) {
        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
        attachProtectedProxy(modelSummaryProxy);
        attachProtectedProxy(fileUploadProxy);
        setupTestRatingEngine(type);
    }

    private void verifyBucketMetadataNotGenerated(RatingEngine testRatingEngine) {
        Map<Long, List<BucketMetadata>> bucketMetadataHistory = bucketedScoreProxy
                .getABCDBucketsByEngineId(mainTestTenant.getId(), testRatingEngine.getId());
        Assert.assertTrue(bucketMetadataHistory.isEmpty());
    }

    private void verifyBucketMetadataGenerated(RatingEngine testRatingEngine) {
        Map<Long, List<BucketMetadata>> bucketMetadataHistory = bucketedScoreProxy
                .getABCDBucketsByEngineId(mainTestTenant.getId(), testRatingEngine.getId());
        Assert.assertNotNull(bucketMetadataHistory);
        Assert.assertEquals(bucketMetadataHistory.size(), 1);
        log.info("time is " + bucketMetadataHistory.keySet().toString());
    }

    private void setupTestSegment() {
        testSegment = constructTargetSegment();
        testSegment = segmentProxy.createOrUpdateSegment(mainTestTenant.getId(), testSegment);
    }

    private void setupSourceFile(CustomEventModelingType type) {
        Resource csvResource = new ClassPathResource("end2end/csv/CustomEventModelTest.csv",
                Thread.currentThread().getContextClassLoader());
        testSourceFile = fileUploadProxy.uploadFile(getSourceFileName(type), false, "CustomEventModelTest.csv",
                SchemaInterpretation.Account, "Account", csvResource);

        FieldMappingDocument fmDoc = fileUploadProxy.getFieldMappings(getSourceFileName(type), "Account");
        fmDoc.setIgnoredFields(new ArrayList<>());
        for (FieldMapping fm : fmDoc.getFieldMappings()) {
            if (fm.getUserField().equals("Event")) {
                fm.setMappedField("Event");
                fm.setMappedToLatticeField(true);
            }
            if (fm.getUserField().equals("name")) {
                fm.setMappedField("CompanyName");
                fm.setMappedToLatticeField(true);
            }
            if (fm.getUserField().equals("SomeRandom")) {
                fm.setMappedField("SomeRandom");
                fm.setMappedToLatticeField(false);
                fmDoc.getIgnoredFields().add("SomeRandom");
            }
            if (fm.getUserField().equals("AnnualRevenue") && type == CustomEventModelingType.CDL) {
                fm.setMappedField("AnnualRevenue");
                fm.setMappedToLatticeField(false);
                fmDoc.getIgnoredFields().add("AnnualRevenue");
            }
            if (fm.getUserField().equals("Industry") && type == CustomEventModelingType.CDL) {
                fm.setMappedField("Industry");
                fm.setMappedToLatticeField(false);
                fmDoc.getIgnoredFields().add("Industry");
            }
        }

        fileUploadProxy.saveFieldMappingDocument(getSourceFileName(type), fmDoc);
    }

    private void setupTestRatingEngine(CustomEventModelingType type) {
        if (type == CustomEventModelingType.CDL) {
            setupTestSegment();
        } else {
            Assert.assertNull(testSegment, "Non-null test segment provided for LPI Style Custom Event Model");
        }
        setupSourceFile(type);

        RatingEngine ratingEngine = constructRatingEngine(RatingEngineType.CUSTOM_EVENT, testSegment);
        RatingEngine testRatingEngine = ratingEngineProxy.createOrUpdateRatingEngine(mainTestTenant.getId(),
                ratingEngine);

        AIModel testAIModel = (AIModel) testRatingEngine.getActiveModel();
        configureCustomEventModel(testAIModel, getSourceFileName(type), type);
        CustomEventModelingConfig advancedConf = CustomEventModelingConfig.getAdvancedModelingConfig(testAIModel);
        advancedConf.setSourceFileName(testSourceFile.getName());
        testAIModel = (AIModel) ratingEngineProxy.updateRatingModel(mainTestTenant.getId(), testRatingEngine.getId(),
                testAIModel.getId(), testAIModel);

        if (type == CustomEventModelingType.CDL) {
            cdlCERatingEngine = testRatingEngine;
            cdlCEAIModel = testAIModel;
        } else {
            lpiCERatingEngine = testRatingEngine;
            lpiCEAIModel = testAIModel;
        }

    }

    private String getSourceFileName(CustomEventModelingType type) {
        return type.name() + "_" + testSourceFileName;
    }

}
