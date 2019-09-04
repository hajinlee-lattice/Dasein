package com.latticeengines.apps.cdl.end2end;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
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
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.lp.BucketedScoreProxy;
import com.latticeengines.testframework.exposed.proxy.pls.ModelSummaryProxy;

public class CustomEventModelEnd2EndDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory
            .getLogger(CustomEventModelEnd2EndDeploymentTestNG.class);
    private static final boolean USE_EXISTING_TENANT = false;
    private static final String EXISTING_TENANT = "JLM1540406334891";
    private static final String LOADING_CHECKPOINT = UpdateTransactionDeploymentTestNG.CHECK_POINT;

    private MetadataSegment testSegment;
    private RatingEngine lpiCERatingEngine;
    private RatingEngine cdlCERatingEngine;
    private AIModel lpiCEAIModel;
    private AIModel cdlCEAIModel;
    private SourceFile testSourceFile;
    private final String testSourceFileName = "CustomEventModelE2ETestFile.csv";
    private CustomEventModelingType testType;
    private final Map<String, Category> refinedAttributes = new HashMap<>();

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private BucketedScoreProxy bucketedScoreProxy;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private Configuration distCpConfiguration;

    @Value("${aws.customer.s3.bucket}")
    private String customerS3Bucket;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @BeforeClass(groups = { "end2end", "manual", "precheckin" })
    public void setup() {
        testType = CustomEventModelingType.CDL;
    }

    /**
     * This test is part of CD pipeline and Trunk Health
     */
    @Test(groups = { "end2end", "precheckin" })
    public void end2endCDLStyleCustomEventModelTest() throws Exception {
        Map<String, Boolean> featureFlagMap = new HashMap<>();
        featureFlagMap.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA.getName(), false);
        setupEnd2EndTestEnvironment(featureFlagMap);
        resumeCheckpoint(LOADING_CHECKPOINT);
        bootstrap(testType);
        runCustomEventModel(testType);
    }

    /**
     * This test is part of CD pipeline and Trunk Health
     */
    @Test(groups = { "end2end", "precheckin" }, dependsOnMethods = "end2endCDLStyleCustomEventModelTest")
    public void end2endCDLStyleCustomEventReModelTest() {
        moveCustomerDataToS3();
        runCustomEventRemodel(testType);
    }

    private void moveCustomerDataToS3() {
        try {
            HdfsToS3PathBuilder builder = new HdfsToS3PathBuilder(useEmr);
            CustomerSpace space = CustomerSpace.parse(mainTestTenant.getId());
            String podId = CamilleEnvironment.getPodId();
            String hdfsAnalyticsDir = builder.getHdfsAnalyticsDir(space.toString());
            String hdfsDataDir = builder.getHdfsAtlasTablesDir(podId, space.getTenantId());

            String s3DataDir = builder.exploreS3FilePath(hdfsDataDir, customerS3Bucket);
            System.out.println("HDFS Path=" + hdfsDataDir);
            System.out.println("S3 Path=" + s3DataDir);
            HdfsUtils.copyGlobToDirWithScheme(distCpConfiguration, hdfsDataDir + "/Account*",
                    s3DataDir, "");
            HdfsUtils.copyGlobToDirWithScheme(distCpConfiguration, hdfsDataDir + "/BucketedAccount*",
                    s3DataDir, "");
            HdfsUtils.copyGlobToDirWithScheme(distCpConfiguration,
                    hdfsDataDir + "/AnalyticPurchaseState*", s3DataDir, "");
            HdfsUtils.copyGlobToDirWithScheme(distCpConfiguration,
                    hdfsDataDir + "/SortedContact*", s3DataDir, "");
            HdfsUtils.copyGlobToDirWithScheme(distCpConfiguration,
                    hdfsDataDir + "/SortedProduct*", s3DataDir, "");
            HdfsUtils.copyGlobToDirWithScheme(distCpConfiguration,
                    hdfsDataDir + "/Aggregated*", s3DataDir, "");
            HdfsUtils.copyGlobToDirWithScheme(distCpConfiguration,
                    hdfsDataDir + "/Calculated**", s3DataDir, "");
            HdfsUtils.rmdir(yarnConfiguration, hdfsAnalyticsDir);
            HdfsUtils.rmdir(yarnConfiguration, hdfsDataDir);
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage());
            throw new RuntimeException(ex);
        }
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
            mainCustomerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        } else {
            Map<String, Boolean> featureFlagMap = new HashMap<>();
            featureFlagMap.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA.getName(), false);
            setupEnd2EndTestEnvironment(featureFlagMap);
            resumeCheckpoint(LOADING_CHECKPOINT);
        }
        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
        testType = CustomEventModelingType.CDL;
        bootstrap(testType);
        runCustomEventModel(testType);
    }

    private void runCustomEventModel(CustomEventModelingType type) {
        log.info("Starting Custom Event modeling ...");
        RatingEngine testRatingEngine = type == CustomEventModelingType.CDL ? cdlCERatingEngine
                : lpiCERatingEngine;
        RatingModel testAIModel = type == CustomEventModelingType.CDL ? cdlCEAIModel : lpiCEAIModel;
        Assert.assertTrue(ratingEngineProxy.validateForModeling(mainTestTenant.getId(),
                testAIModel.getId(), testRatingEngine.getId(), testRatingEngine));
        verifyBucketMetadataNotGenerated(testRatingEngine);
        String modelingWorkflowApplicationId = ratingEngineProxy.modelRatingEngine(
                mainTestTenant.getId(), testRatingEngine.getId(), testAIModel.getId(), null,
                "ga_dev@lattice-engines.com");
        log.info(String.format("Workflow application id is %s", modelingWorkflowApplicationId));
        testRatingEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(),
                testRatingEngine.getId());
        JobStatus completedStatus = waitForWorkflowStatus(modelingWorkflowApplicationId, false);
        testAIModel = ratingEngineProxy.getRatingModel(mainCustomerSpace, testRatingEngine.getId(),
                testAIModel.getId());
        Assert.assertEquals(((AIModel) testAIModel).getModelingJobStatus(), completedStatus);
        Assert.assertEquals(completedStatus, JobStatus.COMPLETED);
        verifyBucketMetadataGenerated(testRatingEngine);
        Assert.assertEquals(ratingEngineProxy
                .getRatingEngine(mainTestTenant.getId(), testRatingEngine.getId()).getStatus(),
                RatingEngineStatus.INACTIVE);
    }

    private void runCustomEventRemodel(CustomEventModelingType type) {
        log.info("Starting Custom Event remodeling ...");
        RatingEngine testRatingEngine = type == CustomEventModelingType.CDL ? cdlCERatingEngine
                : lpiCERatingEngine;
        AIModel testAIModel = type == CustomEventModelingType.CDL ? cdlCEAIModel : lpiCEAIModel;
        AIModel testCERemodel = new AIModel();
        testCERemodel.setRatingEngine(testRatingEngine);
        testCERemodel.setAdvancedModelingConfig(testAIModel.getAdvancedModelingConfig());
        testCERemodel.setDerivedFromRatingModel(testAIModel.getId());
        testCERemodel = (AIModel) ratingEngineProxy.createModelIteration(mainTestTenant.getId(),
                testRatingEngine.getId(), testCERemodel);

        Assert.assertNotEquals(
                ((CustomEventModelingConfig) testAIModel.getAdvancedModelingConfig())
                        .getSourceFileName(),
                ((CustomEventModelingConfig) testCERemodel.getAdvancedModelingConfig())
                        .getSourceFileName());
        testRatingEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(),
                testRatingEngine.getId());
        Assert.assertEquals(testRatingEngine.getLatestIteration().getId(), testCERemodel.getId());

        List<ColumnMetadata> attrs = ratingEngineProxy.getIterationMetadata(mainTestTenant.getId(),
                testRatingEngine.getId(), testAIModel.getId(), "CDL,DataCloud");
        Assert.assertNotNull(attrs);

        verifyBucketMetadataGenerated(testRatingEngine);
        String modelingWorkflowApplicationId = ratingEngineProxy.modelRatingEngine(
                mainTestTenant.getId(), testRatingEngine.getId(), testCERemodel.getId(),
                refineAttributes(attrs), "some@email.com");
        log.info(String.format("Remodel workflow application id is %s",
                modelingWorkflowApplicationId));
        JobStatus completedStatus = waitForWorkflowStatus(modelingWorkflowApplicationId, false);
        Assert.assertEquals(completedStatus, JobStatus.COMPLETED);
        testCERemodel = (AIModel) ratingEngineProxy.getRatingModel(mainCustomerSpace,
                testRatingEngine.getId(), testCERemodel.getId());
        Assert.assertEquals(testCERemodel.getModelingJobStatus(), completedStatus);
        Assert.assertEquals(ratingEngineProxy
                .getRatingEngine(mainTestTenant.getId(), testRatingEngine.getId()).getStatus(),
                RatingEngineStatus.INACTIVE);
        verifyBucketMetadataGeneratedAfterRemodel(testRatingEngine);

        attrs = ratingEngineProxy.getIterationMetadata(mainTestTenant.getId(),
                testRatingEngine.getId(), testCERemodel.getId(), "CDL,DataCloud");
        Assert.assertNotNull(attrs);
        verifyRefinedAttributes(attrs);
    }

    private void verifyRefinedAttributes(List<ColumnMetadata> attrs) {
        for (String refinedAttribute : refinedAttributes.keySet()) {
            ColumnMetadata cm = attrs.stream()
                    .filter(attr -> attr.getAttrName().equals(refinedAttribute)).findFirst().get();
            Assert.assertEquals(cm.getApprovedUsageList().size(), 1);
            Assert.assertEquals(cm.getApprovedUsageList().get(0), ApprovedUsage.NONE,
                    "Failed to assert ApprovedUsage of attribute: " + refinedAttribute);
        }
    }

    private List<ColumnMetadata> refineAttributes(List<ColumnMetadata> attrs) {
        int noOfAttributesToRefine = 3;
        for (ColumnMetadata attr : attrs) {
            if (attr.getImportanceOrdering() != null) {
                refinedAttributes.put(attr.getAttrName(), attr.getCategory());
                attr.setApprovedUsageList(Collections.singletonList(ApprovedUsage.NONE));
                noOfAttributesToRefine--;
                log.info("Refined Attr: " + attr.getAttrName());
            }
            if (noOfAttributesToRefine == 0) {
                return attrs;
            }
        }

        return attrs;
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

    private void verifyBucketMetadataGeneratedAfterRemodel(RatingEngine testRatingEngine) {
        Map<Long, List<BucketMetadata>> bucketMetadataHistory = bucketedScoreProxy
                .getABCDBucketsByEngineId(mainTestTenant.getId(), testRatingEngine.getId());
        Assert.assertNotNull(bucketMetadataHistory);
        Assert.assertEquals(bucketMetadataHistory.size(), 2);
        log.info("time is " + bucketMetadataHistory.keySet().toString());
    }

    private void setupTestSegment() {
        testSegment = constructTargetSegment();
        testSegment = segmentProxy.createOrUpdateSegment(mainTestTenant.getId(), testSegment);
    }

    private void setupSourceFile(CustomEventModelingType type) {
        Resource csvResource = new ClassPathResource("end2end/csv/CustomEventModelTest.csv",
                Thread.currentThread().getContextClassLoader());
        SchemaInterpretation schemaInterpretation = type == CustomEventModelingType.CDL
                ? SchemaInterpretation.Account
                : SchemaInterpretation.SalesforceAccount;
        String entity = type == CustomEventModelingType.CDL ? SchemaInterpretation.Account.name()
                : null;
        testSourceFile = fileUploadProxy.uploadFile(getSourceFileName(type), false,
                testSourceFileName, schemaInterpretation, entity, csvResource);

        FieldMappingDocument fmDoc = fileUploadProxy.getFieldMappings(getSourceFileName(type),
                schemaInterpretation.name());
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
            Assert.assertNull(testSegment,
                    "Non-null test segment provided for LPI Style Custom Event Model");
        }
        setupSourceFile(type);

        RatingEngine ratingEngine = constructRatingEngine(RatingEngineType.CUSTOM_EVENT,
                testSegment);
        RatingEngine testRatingEngine = ratingEngineProxy
                .createOrUpdateRatingEngine(mainTestTenant.getId(), ratingEngine);

        AIModel testAIModel = (AIModel) testRatingEngine.getLatestIteration();
        configureCustomEventModel(testAIModel, getSourceFileName(type), type);
        CustomEventModelingConfig advancedConf = CustomEventModelingConfig
                .getAdvancedModelingConfig(testAIModel);
        advancedConf.setSourceFileName(testSourceFile.getName());
        testAIModel = (AIModel) ratingEngineProxy.updateRatingModel(mainTestTenant.getId(),
                testRatingEngine.getId(), testAIModel.getId(), testAIModel);

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
