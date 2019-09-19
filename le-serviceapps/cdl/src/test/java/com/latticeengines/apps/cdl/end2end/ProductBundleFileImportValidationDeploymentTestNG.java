package com.latticeengines.apps.cdl.end2end;


import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ModelingStrategy;
import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.lp.CreateBucketMetadataRequest;
import com.latticeengines.domain.exposed.util.BucketMetadataUtils;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.lp.BucketedScoreProxy;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class ProductBundleFileImportValidationDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ProductBundleFileImportValidationDeploymentTestNG.class);
    private String customerSpace;
    private static final String MODELS_RESOURCE_ROOT = "end2end/models";
    private static final String S3_ATLAS_DATA_TABLE_DIR = "/%s/atlas/Data/Tables";
    private static final String HDFS_DATA_TABLE_DIR = "/Pods/%s/Contracts/%s/Tenants/%s/Spaces/Production/Data/Tables";

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Inject
    private BucketedScoreProxy bucketedScoreProxy;

    @Inject
    private WorkflowProxy workflowProxy;

    @Inject
    private S3Service s3Service;

    @Value("${aws.customer.s3.bucket}")
    private String bucket;

    @Value("${camille.zk.pod.id}")
    protected String podId;

    // Target Products are shared with CrossSellModelEnd2EndDeploymentTestNG RefreshRatingDeploymentTestNG
    private static final ImmutableList<String> targetProducts = ImmutableList.of("1iHa3C9UQFBPknqKCNW3L6WgUAARc4o");
    @Override
    @BeforeClass(groups = "end2end")
    public void setup() throws Exception {
        setupEnd2EndTestEnvironment();
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();

    }

    @Test(groups = "end2end")
    public void testCurrentBundle() throws Exception {
        // get current bundle before PA
        byte[] bytes = getCurrentBundleResponse();
        Assert.assertTrue(bytes == null);
        resumeCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);

        // get current bundle after PA
        byte[] bytes2 = getCurrentBundleResponse();
        Assert.assertTrue(bytes2.length > 0);
        CSVFormat format = LECSVFormat.format;
        try (CSVParser parser = new CSVParser(new InputStreamReader(new ByteArrayInputStream(bytes2)), format)){
            Set<String> csvHeaders = parser.getHeaderMap().keySet();
            assertTrue(csvHeaders.contains("Product Id"));
            assertTrue(csvHeaders.contains("Product Name"));
            assertTrue(csvHeaders.contains("Product Bundle"));
            assertTrue(csvHeaders.contains("Description"));
        } catch (Exception e) {
            // unexpected exception happened
        }

    }

    private byte[] getCurrentBundleResponse() {
        RestTemplate template = testBed.getRestTemplate();
        String url = String.format("%s/pls/datafiles/bundlecsv", deployedHostPort);
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Arrays.asList(MediaType.ALL));
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<byte[]> response = template.exchange(url, HttpMethod.GET, entity, byte[].class);
        String fileName = response.getHeaders().getFirst("Content-Disposition");
        Assert.assertTrue(fileName.contains(".csv"));
        return response.getBody();
    }

    @Test(groups = "end2end", dependsOnMethods = "testCurrentBundle")
    public void testProductBundle() throws Exception {
        // create bundle related segment
        createTestSegmentProductBundle();

        // segment2 and segment3 has common attribute AttributeLookup(BusinessEntity.Account, "State")
        createTestSegment2();
        createTestSegment3();

        // mock one active x-shell rating engine
        createModelingSegment();
        MetadataSegment segment = segmentProxy.getMetadataSegmentByName(customerSpace, SEGMENT_NAME_MODELING);

        // create AI model
        Thread setupAIModelsThread = new Thread(this::setupAIModels);
        setupAIModelsThread.start();
        if(setupAIModelsThread != null) {
            setupAIModelsThread.join();
        }
        modelSummaryProxy.downloadModelSummary(mainCustomerSpace);

        List<ModelSummary> summaries = modelSummaryProxy.getModelSummaries(customerSpace, null);
        Assert.assertNotNull(summaries);
        ModelSummary summary = summaries.get(0);

        RatingEngine ai = createCrossSellEngine(segment, summary, PredictionType.EXPECTED_VALUE);
        activateRatingEngine(ai.getId());
        ApplicationId applicationId = importDataWithApplicationId(BusinessEntity.Product, "ProductBundles_Validations.csv", null,
                false, false);
        JobStatus status = waitForWorkflowStatus(applicationId.toString(), false);
        Assert.assertEquals(status, JobStatus.FAILED);
        Job job = workflowProxy.getWorkflowJobFromApplicationId(applicationId.toString(), customerSpace);
        Assert.assertEquals(job.getErrorMsg(), "Import failed because there were 4 errors : 3 missing product bundles" +
                " in use (this import will completely replace the previous one), 2 product bundle has different product SKUs. Dependant models will need to be remodelled to get accurate scores. error when validating with input file, please reference error.csv for details.");
        List<?> rawList = JsonUtils.deserialize(job.getOutputs().get("DATAFEEDTASK_IMPORT_ERROR_FILES"), List.class);
        String errorFile = JsonUtils.convertList(rawList, String.class).get(0);
        Assert.assertNotNull(errorFile);
        String tenantId = CustomerSpace.parse(mainCustomerSpace).getTenantId();
        String s3File = String.format(S3_ATLAS_DATA_TABLE_DIR, tenantId) +
                        errorFile.substring(String.format(HDFS_DATA_TABLE_DIR, podId, tenantId, tenantId).length());
        Assert.assertTrue(s3Service.objectExist(bucket, s3File));
    }

    private void setupAIModels() {
        uploadModel(MODELS_RESOURCE_ROOT + "/ev_model.tar.gz");
    }

    @SuppressWarnings("deprecation")
    private RatingEngine createCrossSellEngine(MetadataSegment segment, ModelSummary modelSummary,
                                               PredictionType predictionType) throws InterruptedException {
        RatingEngine ratingEngine = constructRatingEngine(RatingEngineType.CROSS_SELL, segment);

        RatingEngine newEngine = ratingEngineProxy.createOrUpdateRatingEngine(mainTestTenant.getId(), ratingEngine);
        newEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), newEngine.getId());
        assertNotNull(newEngine);
        Assert.assertNotNull(newEngine.getLatestIteration(), JsonUtils.pprint(newEngine));
        log.info("Created rating engine " + newEngine.getId());

        AIModel model = (AIModel) newEngine.getLatestIteration();
        configureCrossSellModel(model, predictionType, ModelingStrategy.CROSS_SELL_FIRST_PURCHASE, targetProducts,
                targetProducts);
        model.setModelSummaryId(modelSummary.getId());

        ratingEngineProxy.updateRatingModel(mainTestTenant.getId(), newEngine.getId(), model.getId(), model);
        log.info("Updated rating model " + model.getId());

        ratingEngineProxy.setScoringIteration(mainCustomerSpace, newEngine.getId(), model.getId(),
                BucketMetadataUtils.getDefaultMetadata(), null);
        Thread.sleep(300);
        insertBucketMetadata(modelSummary.getId(), newEngine.getId());
        Thread.sleep(300);
        return ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), newEngine.getId());
    }

    private void insertBucketMetadata(String modelGuid, String engineId) {
        CreateBucketMetadataRequest request = new CreateBucketMetadataRequest();
        request.setModelGuid(modelGuid);
        request.setRatingEngineId(engineId);
        request.setBucketMetadataList(getModifiedBucketMetadata());
        request.setLastModifiedBy(TestFrameworkUtils.SUPER_ADMIN_USERNAME);
        bucketedScoreProxy.createABCDBuckets(mainTestTenant.getId(), request);
    }


    private List<BucketMetadata> getModifiedBucketMetadata() {
        List<BucketMetadata> buckets = new ArrayList<>();
        buckets.add(BucketMetadataUtils.bucket(99, 90, BucketName.A));
        buckets.add(BucketMetadataUtils.bucket(90, 85, BucketName.B));
        buckets.add(BucketMetadataUtils.bucket(85, 40, BucketName.C));
        buckets.add(BucketMetadataUtils.bucket(40, 5, BucketName.D));
        long currentTime = System.currentTimeMillis();
        buckets.forEach(bkt -> bkt.setCreationTimestamp(currentTime));
        return buckets;
    }
}
