package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.modeling.factory.AlgorithmFactory;
import com.latticeengines.domain.exposed.modeling.factory.DataFlowFactory;
import com.latticeengines.domain.exposed.modeling.factory.SamplingFactory;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.pls.CloneModelingParameters;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.VdbMetadataField;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.scoringapi.Fields;
import com.latticeengines.domain.exposed.scoringapi.Model;
import com.latticeengines.domain.exposed.scoringapi.ModelDetail;
import com.latticeengines.domain.exposed.scoringapi.ModelType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.pls.PlsInternalProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component
public class SelfServiceModelingEndToEndDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final String RESOURCE_BASE = "com/latticeengines/pls/end2end/selfServiceModeling/csvfiles";
    private static final Logger log = LoggerFactory.getLogger(SelfServiceModelingEndToEndDeploymentTestNG.class);

    @Inject
    private WorkflowProxy workflowProxy;

    @Inject
    private PlsInternalProxy plsInternalProxy;

    @Inject
    protected ScoreCorrectnessService scoreCompareService;

    private Tenant firstTenant;
    private Tenant secondTenant;

    protected String fileName;
    protected String modelName = this.getClass().getSimpleName() + "_" + DateTime.now().getMillis();
    protected String modelDisplayName;
    protected SourceFile sourceFile;
    protected SchemaInterpretation schemaInterpretation;
    protected ModelingParameters parameters;
    protected ModelSummary originalModelSummary;

    private String modelingWorkflowApplicationId;
    private ModelSummary copiedModelSummary;
    private ModelSummary clonedModelSummary;
    private ModelSummary replacedModelSummary;

    @Value("${aws.customer.s3.bucket}")
    private String customerS3Bucket;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Inject
    protected Configuration yarnConfiguration;

    @BeforeClass(groups = { "deployment.lp", "precheckin" })
    public void setup() throws Exception {
        log.info("Bootstrapping test tenants using tenant console ...");
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
        firstTenant = testBed.getMainTestTenant();

        // if (EncryptionGlobalState.isEnabled()) {
        // ConfigurationController<CustomerSpaceScope> controller =
        // ConfigurationController
        // .construct(new
        // CustomerSpaceScope(CustomerSpace.parse(firstTenant.getId())));
        // assertTrue(controller.exists(new Path("/EncryptionKey")));
        // }

        // Create second tenant for copy model use case
        testBed.bootstrapForProduct(LatticeProduct.LPA3);
        saveAttributeSelection(CustomerSpace.parse(firstTenant.getName()));
        secondTenant = testBed.getTestTenants().get(1);
        fileName = "Hootsuite_PLS132_LP3_ScoringLead_20160330_165806_modified.csv";
        modelDisplayName = "Self Service Modeling Test Display Name";
        schemaInterpretation = SchemaInterpretation.SalesforceLead;

        log.info("Test environment setup finished.");

        testBed.excludeTestTenantsForCleanup(Collections.singletonList(firstTenant));
    }

    public String getScoringApiInternalUrl() {
        return getRestAPIHostPort() + "/pls/scoringapi-internal";
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = { "deployment.lp", "precheckin" }, enabled = true)
    public void uploadFile() {
        log.info("uploading file for modeling...");
        LinkedMultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
        map.add("file", new ClassPathResource(RESOURCE_BASE + "/" + fileName));
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<>(map, headers);
        ResponseDocument response = restTemplate.postForObject( //
                String.format("%s/pls/models/uploadfile/unnamed?displayName=%s", getRestAPIHostPort(),
                        "SelfServiceModeling Test File.csv"), //
                requestEntity, ResponseDocument.class);
        sourceFile = new ObjectMapper().convertValue(response.getResult(), SourceFile.class);
        log.info(sourceFile.getName());

        map = new LinkedMultiValueMap<>();
        map.add("metadataFile", new ClassPathResource(
                "com/latticeengines/pls/end2end/selfServiceModeling/pivotmappingfiles/pivotvalues.txt.gz"));
        headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        requestEntity = new HttpEntity<>(map, headers);

        response = restTemplate.postForObject( //
                String.format("%s/pls/metadatauploads/modules/%s/%s?artifactName=%s&compressed=%s",
                        getRestAPIHostPort(), "module1", "pivotmappings", "pivotvalues", "true"), //
                requestEntity, ResponseDocument.class);
        String pivotFilePath = new ObjectMapper().convertValue(response.getResult(), String.class);
        log.info(pivotFilePath);
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = { "deployment.lp", "precheckin" }, enabled = true, dependsOnMethods = "uploadFile")
    public void resolveMetadata() {
        log.info("Resolving metadata for modeling ...");
        parameters = createModelingParameters();

        sourceFile.setSchemaInterpretation(schemaInterpretation);
        ResponseDocument response = restTemplate.postForObject(
                String.format("%s/pls/models/uploadfile/%s/fieldmappings?schema=%s", getRestAPIHostPort(),
                        sourceFile.getName(), schemaInterpretation.name()),
                parameters, ResponseDocument.class);
        FieldMappingDocument mappings = new ObjectMapper().convertValue(response.getResult(),
                FieldMappingDocument.class);

        for (FieldMapping mapping : mappings.getFieldMappings()) {
            if (mapping.getMappedField() == null) {
                mapping.setMappedToLatticeField(false);
                mapping.setMappedField(mapping.getUserField().replace(' ', '_'));
            }
            if (mapping.getMappedField().startsWith("Activity_Count")) {
                mapping.setFieldType(UserDefinedType.NUMBER);
            }
        }

        List<String> ignored = new ArrayList<>();
        ignored.add("Activity_Count_Interesting_Moment_Webinar");
        mappings.setIgnoredFields(ignored);

        log.info("the fieldmappings are: " + mappings.getFieldMappings());
        log.info("the ignored fields are: " + mappings.getIgnoredFields());
        restTemplate.postForObject(String.format("%s/pls/models/uploadfile/fieldmappings?displayName=%s",
                getRestAPIHostPort(), sourceFile.getName()), mappings, Void.class);
    }

    protected ModelingParameters createModelingParameters() {
        ModelingParameters parameters = new ModelingParameters();
        parameters.setName(modelName);
        parameters.setDisplayName(modelDisplayName);
        parameters.setDescription("Test");
        parameters.setModuleName("module1");
        parameters.setPivotFileName("pivotvalues.csv");
        parameters.setFilename(sourceFile.getName());
        parameters.setActivateModelSummaryByDefault(true);
        Map<String, String> runtimeParams = new HashMap<>();
        runtimeParams.put(SamplingFactory.MODEL_SAMPLING_SEED_KEY, "987654");
        runtimeParams.put(AlgorithmFactory.RF_SEED_KEY, "987654");
        runtimeParams.put(DataFlowFactory.DATAFLOW_DO_SORT_FOR_ATTR_FLOW, "");
        parameters.setRunTimeParams(runtimeParams);
        return parameters;
    }

    @Test(groups = { "deployment.cdl", "deployment.lp",
            "precheckin" }, enabled = true, dependsOnMethods = "resolveMetadata")
    public void createModel() {
        modelingWorkflowApplicationId = model(parameters);
    }

    @SuppressWarnings("rawtypes")
    private String model(ModelingParameters parameters) {
        log.info("Start modeling ...");
        ResponseDocument response;
        response = restTemplate.postForObject(
                String.format("%s/pls/models/%s", getRestAPIHostPort(), parameters.getName()), parameters,
                ResponseDocument.class);
        String modelingWorkflowApplicationId = new ObjectMapper().convertValue(response.getResult(), String.class);

        log.info(String.format("Workflow application id is %s", modelingWorkflowApplicationId));
        waitForWorkflowStatus(workflowProxy, modelingWorkflowApplicationId, true);

        boolean thrown = false;
        try {
            response = restTemplate.postForObject(
                    String.format("%s/pls/models/%s", getRestAPIHostPort(), UUID.randomUUID()), parameters,
                    ResponseDocument.class);
        } catch (Exception e) {
            thrown = true;
        }

        assertTrue(thrown);

        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, modelingWorkflowApplicationId, false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
        return modelingWorkflowApplicationId;
    }

    @Test(groups = { "deployment.cdl", "deployment.lp" }, dependsOnMethods = "createModel", enabled = true)
    public void retrieveReport() {
        log.info("Retrieving report for modeling ...");
        Job job = restTemplate.getForObject( //
                String.format("%s/pls/jobs/yarnapps/%s", getRestAPIHostPort(), modelingWorkflowApplicationId), //
                Job.class);
        assertNotNull(job);
        List<Report> reports = job.getReports();
        assertEquals(reports.size(), 2);
    }

    @Test(groups = { "deployment.cdl", "deployment.lp", "precheckin" }, dependsOnMethods = "createModel")
    public void retrieveModelSummary() throws InterruptedException {
        log.info("Retrieving model summary for modeling ...");
        originalModelSummary = waitToDownloadModelSummary(modelName);
        assertNotNull(originalModelSummary);
        assertEquals(originalModelSummary.getSourceSchemaInterpretation(), schemaInterpretation.toString());
        assertNotNull(originalModelSummary.getTrainingTableName());
        assertFalse(originalModelSummary.getTrainingTableName().isEmpty());
        assertEquals(originalModelSummary.getModelSummaryConfiguration()
                .getString(ProvenancePropertyName.TransformationGroupName, null), TransformationGroup.ALL.getName());
        assertJobExistsWithModelIdAndModelName(originalModelSummary.getId());
        inspectOriginalModelSummaryPredictors(originalModelSummary);
        assertABCDBucketsCreated(originalModelSummary.getId());
        log.info("Finished retrieveModelSummary");
    }

    void activateModelSummary(String modelId) throws InterruptedException {
        log.info("Update model " + modelId + " to active.");
        String modelApi = getRestAPIHostPort() + "/pls/modelsummaries/" + modelId;
        AttributeMap attrMap = new AttributeMap();
        attrMap.put("Status", ModelSummaryStatus.ACTIVE.getStatusCode());
        HttpEntity<AttributeMap> requestEntity = new HttpEntity<>(attrMap);
        restTemplate.exchange(modelApi, HttpMethod.PUT, requestEntity, Object.class);
        // Look up the model summary with details
        ModelSummary summary = restTemplate.getForObject(
                String.format("%s/pls/modelsummaries/%s", getRestAPIHostPort(), modelId), ModelSummary.class);
        assertEquals(summary.getStatus(), ModelSummaryStatus.ACTIVE);
    }

    @Test(groups = { "deployment.lp", "precheckin" }, enabled = true, dependsOnMethods = "retrieveModelSummary")
    public void testScoringApiInternalModels() {
        log.info("Retrieving Models via ScoringAPI Proxy.");
        String url = getScoringApiInternalUrl() + "/models";
        List<?> resultList = restTemplate.getForObject(url, List.class);
        assertNotNull(resultList);
        List<Model> modelList = new ArrayList<>();
        if (resultList != null) {
            for (Object obj : resultList) {
                String json = JsonUtils.serialize(obj);
                Model model = JsonUtils.deserialize(json, Model.class);
                modelList.add(model);
            }
        }
        assertEquals(modelList.size(), 1);
        log.info("Retrieving Model Fields via ScoringAPI Proxy.");

        log.info("Retrieving Models via ScoringAPI Proxy with Type Filter.");
        url = getScoringApiInternalUrl() + "/models?type=" + ModelType.CONTACT;
        resultList = restTemplate.getForObject(url, List.class);
        assertNotNull(resultList);
        modelList = new ArrayList<>();
        if (resultList != null) {
            for (Object obj : resultList) {
                String json = JsonUtils.serialize(obj);
                Model model = JsonUtils.deserialize(json, Model.class);
                modelList.add(model);
            }
        }
        log.info("Retrieving Model Fields via ScoringAPI Proxy with Type Filter.");
    }

    @Test(groups = { "deployment.lp", "precheckin" }, enabled = true, dependsOnMethods = "testScoringApiInternalModels")
    public void testScoringApiInternalModelDetails() {
        log.info("Retrieving ModelDetails via ScoringAPI Proxy.");
        String url = getScoringApiInternalUrl()
                + "/modeldetails?considerAllStatus=true&offset=0&maximum=10&considerDeleted=false";
        List<?> resultList = restTemplate.getForObject(url, List.class);
        assertNotNull(resultList);
        List<ModelDetail> paginatedModels = new ArrayList<>();
        if (resultList != null) {
            for (Object obj : resultList) {
                String json = JsonUtils.serialize(obj);
                ModelDetail modelDetail = JsonUtils.deserialize(json, ModelDetail.class);
                paginatedModels.add(modelDetail);
            }
        }
        assertEquals(paginatedModels.size(), 1);
        assertNotNull(paginatedModels.get(0).getModel());
        assertEquals(paginatedModels.get(0).getModel().getName(), modelDisplayName);
        log.info("Finished Retrieving ModelDetails via ScoringAPI Proxy.");

        log.info("Retrieving Model Fields via ScoringAPI Proxy.");
        String fieldsUrl = getScoringApiInternalUrl() + "/models/" + paginatedModels.get(0).getModel().getModelId()
                + "/fields";
        Fields fields = restTemplate.getForObject(fieldsUrl, Fields.class);
        assertNotNull(fields);
        assertNotNull(fields.getFields());
        assertEquals(fields.getModelId(), paginatedModels.get(0).getModel().getModelId());
        log.info("Retrieving Model Fields via ScoringAPI Proxy.");
    }

    @Test(groups = { "deployment.lp", "precheckin" }, enabled = true, dependsOnMethods = "retrieveModelSummary")
    public void compareRtsScoreWithModelingForOriginalModelSummary() throws IOException, InterruptedException {
        log.info("Start compareRtsScoreWithModelingForOriginalModelSummary");
        compareRtsScoreWithModeling(originalModelSummary, 843, firstTenant.getId());
        log.info("Finished compareRtsScoreWithModelingForOriginalModelSummary");
    }

    private void assertABCDBucketsCreated(String modelId) {
        log.info(String.format("Retrieving ABCD Buckets for model: %s", modelId));
        Map<?, ?> bucketMetadataRaw = restTemplate.getForObject(
                String.format("%s/pls/bucketedscore/abcdbuckets/%s", getRestAPIHostPort(), modelId), Map.class);
        Assert.assertNotNull(bucketMetadataRaw);
        log.info("The bucket creation time to buckets are: " + JsonUtils.serialize(bucketMetadataRaw));
        log.info(String.format("The timestamps are: %s", JsonUtils.serialize(bucketMetadataRaw.keySet())));
        @SuppressWarnings("rawtypes")
        Map<Long, List> creationTimeToBucketMetadatas = JsonUtils.convertMap(bucketMetadataRaw, Long.class, List.class);

        assertEquals(creationTimeToBucketMetadatas.keySet().size(), 1);
        Long timestamp = (Long) creationTimeToBucketMetadatas.keySet().toArray()[0];
        List<BucketMetadata> bucketMetadatas = JsonUtils.convertList(creationTimeToBucketMetadatas.get(timestamp),
                BucketMetadata.class);
        Set<BucketName> bucketNames = new HashSet<>(
                Arrays.asList(BucketName.A, BucketName.B, BucketName.C, BucketName.D));
        for (BucketMetadata bucketMetadata : bucketMetadatas) {
            switch (bucketMetadata.getBucket()) {
            case A:
                bucketNames.remove(bucketMetadata.getBucket());
                assertEquals(bucketMetadata.getLeftBoundScore(), 99);
                assertEquals(bucketMetadata.getRightBoundScore(), 95);
                break;
            case B:
                bucketNames.remove(bucketMetadata.getBucket());
                assertEquals(bucketMetadata.getLeftBoundScore(), 94);
                assertEquals(bucketMetadata.getRightBoundScore(), 85);
                break;
            case C:
                bucketNames.remove(bucketMetadata.getBucket());
                assertEquals(bucketMetadata.getLeftBoundScore(), 84);
                assertEquals(bucketMetadata.getRightBoundScore(), 50);
                break;
            case D:
                bucketNames.remove(bucketMetadata.getBucket());
                assertEquals(bucketMetadata.getLeftBoundScore(), 49);
                assertEquals(bucketMetadata.getRightBoundScore(), 5);
                break;
            default:
                Assert.fail("Should not see the bucket name " + bucketMetadata.getBucket());
            }
        }
        assertTrue(bucketNames.isEmpty());
    }

    @Test(groups = { "deployment.cdl", "deployment.lp" }, enabled = true, dependsOnMethods = "createModel")
    public void retrieveErrorsFile() {
        log.info("Retrieving the error file ...");
        // Relies on error in Account.csv
        restTemplate.getMessageConverters().add(new ByteArrayHttpMessageConverter());
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Arrays.asList(MediaType.ALL));
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<byte[]> response = restTemplate.exchange(
                String.format("%s/pls/fileuploads/%s/import/errors", getRestAPIHostPort(), sourceFile.getName()),
                HttpMethod.GET, entity, byte[].class);
        assertEquals(response.getStatusCode(), HttpStatus.OK);
        String errors = new String(response.getBody());
        assertTrue(errors.length() > 0);
        log.info("Finished retrieveErrorsFile");
    }

    @Test(groups = "deployment.lp", enabled = true, timeOut = 3600000, dependsOnMethods = "retrieveModelSummary")
    public void copyModel() {
        cleanCustomerData(firstTenant.getId());
        copyModel(originalModelSummary.getId(), secondTenant.getId());
    }

    private void cleanCustomerData(String tenantId) {
        try {
            HdfsToS3PathBuilder builder = new HdfsToS3PathBuilder(useEmr);
            CustomerSpace space = CustomerSpace.parse(tenantId);
            String podId = CamilleEnvironment.getPodId();
            String hdfsAnalyticsDir = builder.getHdfsAnalyticsDir(space.toString());
            String hdfsDataDir = builder.getHdfsAtlasTablesDir(podId, space.getTenantId());

            String s3DataDir = builder.exploreS3FilePath(hdfsDataDir, customerS3Bucket);
            System.out.println("HDFS Path=" + hdfsDataDir);
            System.out.println("S3 Path=" + s3DataDir);
            HdfsUtils.rmdir(yarnConfiguration, hdfsAnalyticsDir);
            HdfsUtils.rmdir(yarnConfiguration, hdfsDataDir);
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage());
            throw new RuntimeException(ex);
        }
    }

    void copyModel(String originalModelId, String targetTenantId) {
        log.info("Copy the model that is created ...");
        ResponseDocument<?> response = getRestTemplate().postForObject(
                String.format("%s/pls/models/copymodel/%s?targetTenantId=%s", getRestAPIHostPort(), originalModelId,
                        targetTenantId), //
                null, ResponseDocument.class);
        Boolean res = new ObjectMapper().convertValue(response.getResult(), Boolean.class);
        assertTrue(res);
        log.info("Finished copyModel");
    }

    @Test(groups = "deployment.lp", dependsOnMethods = "copyModel", timeOut = 1200000, enabled = true)
    public void retrieveModelSummaryForCopiedModel() throws InterruptedException, IOException {
        log.info("Retrieving the copied model summary ...");
        testBed.switchToSuperAdmin(secondTenant);
        copiedModelSummary = waitToDownloadModelSummary(modelName);
        assertNotNull(copiedModelSummary);
        assertEquals(copiedModelSummary.getSourceSchemaInterpretation(),
                originalModelSummary.getSourceSchemaInterpretation());
        assertNotNull(copiedModelSummary.getTrainingTableName());
        assertFalse(copiedModelSummary.getTrainingTableName().isEmpty());
        assertEquals(originalModelSummary.getModelSummaryConfiguration()
                .getString(ProvenancePropertyName.TransformationGroupName, null), TransformationGroup.ALL.getName());

        inspectOriginalModelSummaryPredictors(copiedModelSummary);
        activateModelSummary(copiedModelSummary.getId());
        compareRtsScoreWithModeling(copiedModelSummary, 687, secondTenant.getId());
        assertABCDBucketsCreated(copiedModelSummary.getId());
        log.info("Finished retrieveModelSummaryForCopiedModel");
    }

    @Test(groups = "deployment.lp", enabled = true, dependsOnMethods = { "retrieveModelSummaryForCopiedModel" })
    public void cloneAndRemodel() {
        cloneAndRemodel(copiedModelSummary);
    }

    void cloneAndRemodel(ModelSummary baseModelSummary) {
        log.info("Cloning and remodel the model summary ...");
        @SuppressWarnings("unchecked")
        List<Object> rawFields = restTemplate.getForObject(
                String.format("%s/pls/modelsummaries/metadata/%s", getRestAPIHostPort(), baseModelSummary.getId()),
                List.class);
        List<VdbMetadataField> fields = new ArrayList<>();
        for (Object rawField : rawFields) {
            VdbMetadataField field = JsonUtils.convertValue(rawField, VdbMetadataField.class);
            fields.add(field);

            if (field.getColumnName().equals("Industry_Group")) {
                field.setApprovedUsage(ModelingMetadata.NONE_APPROVED_USAGE);
            }
            if (field.getColumnName().equals("Activity_Count_Click_Email")) {
                field.setApprovedUsage(ModelingMetadata.NONE_APPROVED_USAGE);
            }
        }

        // Now remodel
        CloneModelingParameters parameters = new CloneModelingParameters();
        parameters.setName(modelName + "_clone");
        modelName = parameters.getName();
        parameters.setDisplayName(modelDisplayName);
        parameters.setDescription("clone");
        parameters.setAttributes(fields);
        parameters.setSourceModelSummaryId(baseModelSummary.getId());
        parameters.setDeduplicationType(DedupType.ONELEADPERDOMAIN);
        parameters.setEnableTransformations(true);
        parameters.setExcludePropDataAttributes(true);
        parameters.setActivateModelSummaryByDefault(true);

        ResponseDocument<?> response;
        response = restTemplate.postForObject(String.format("%s/pls/models/%s/clone", getRestAPIHostPort(), modelName),
                parameters, ResponseDocument.class);

        modelingWorkflowApplicationId = new ObjectMapper().convertValue(response.getResult(), String.class);

        log.info(String.format("Workflow application id is %s", modelingWorkflowApplicationId));

        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, modelingWorkflowApplicationId, false,
                secondTenant);
        assertEquals(completedStatus, JobStatus.COMPLETED);
        log.info("Finished cloneAndRemodel");
    }

    @Test(groups = "deployment.lp", enabled = true, dependsOnMethods = "cloneAndRemodel", timeOut = 120000)
    public void retrieveModelSummaryForClonedModel() throws InterruptedException, IOException {
        log.info("Retrieve the model summary after cloning and remodeling ...");
        clonedModelSummary = waitToDownloadModelSummary(modelName);
        assertNotNull(clonedModelSummary);
        assertJobExistsWithModelIdAndModelName(clonedModelSummary.getId());
        List<Predictor> predictors = clonedModelSummary.getPredictors();
        assertTrue(!Iterables.any(predictors, new Predicate<Predictor>() {
            @Override
            public boolean apply(@Nullable Predictor predictor) {
                return predictor.getName().equals(InterfaceName.Website.toString())
                        || predictor.getName().equals(InterfaceName.Country.toString());
            }

        }));
        assertEquals(clonedModelSummary.getSourceSchemaInterpretation(),
                SchemaInterpretation.SalesforceLead.toString());
        assertEquals(clonedModelSummary.getModelSummaryConfiguration()
                .getString(ProvenancePropertyName.TransformationGroupName, null), TransformationGroup.ALL.getName());
        String foundFileTableName = clonedModelSummary.getTrainingTableName();
        assertNotNull(foundFileTableName);

        @SuppressWarnings("unchecked")
        List<Object> rawFields = restTemplate.getForObject(
                String.format("%s/pls/modelsummaries/metadata/%s", getRestAPIHostPort(), clonedModelSummary.getId()),
                List.class);
        Assert.assertNotNull(rawFields);
        assertTrue(rawFields.stream().anyMatch(raw -> {
            VdbMetadataField metadataField = new ObjectMapper().convertValue(raw, VdbMetadataField.class);
            return metadataField.getColumnName().equals("Industry_Group")
                    && metadataField.getApprovedUsage().equals(ModelingMetadata.NONE_APPROVED_USAGE);
        }));
        assertTrue(rawFields.stream().anyMatch(raw -> {
            VdbMetadataField metadataField = new ObjectMapper().convertValue(raw, VdbMetadataField.class);
            return metadataField.getColumnName().equals("Activity_Count_Click_Email")
                    && metadataField.getApprovedUsage().equals(ModelingMetadata.NONE_APPROVED_USAGE);
        }));

        compareRtsScoreWithModeling(clonedModelSummary, 730, secondTenant.getId());
        assertABCDBucketsCreated(clonedModelSummary.getId());
        log.info("Finished retrieveModelSummaryForClonedModel");
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = "deployment.lp", enabled = true, dependsOnMethods = { "scoreTrainingDataOfClonedModel" })
    public void replaceModel() {
        log.info("Replacing the cloned model with original model ...");
        cleanCustomerData(firstTenant.getId());
        testBed.switchToSuperAdmin(firstTenant);
        ResponseDocument response = restTemplate.postForObject(
                String.format("%s/pls/models/replacemodel/%s?targetTenantId=%s&targetModelId=%s", getRestAPIHostPort(),
                        originalModelSummary.getId(), secondTenant.getId(), clonedModelSummary.getId()),
                parameters, ResponseDocument.class);
        Boolean res = new ObjectMapper().convertValue(response.getResult(), Boolean.class);
        Assert.assertTrue(res);
        log.info("Finished replaceModel");
    }

    @Test(groups = "deployment.lp", enabled = true, dependsOnMethods = "replaceModel")
    public void retrieveModelSummaryForReplacedModel() throws InterruptedException {
        log.info("Retrieve the model summary after replacing ...");
        testBed.switchToSuperAdmin(secondTenant);
        replacedModelSummary = waitToDownloadModelSummary(modelName);
        assertNotNull(replacedModelSummary);
        assertEquals(replacedModelSummary.getSourceSchemaInterpretation(),
                SchemaInterpretation.SalesforceLead.toString());
        assertNotNull(replacedModelSummary.getTrainingTableName());
        assertEquals(replacedModelSummary.getTrainingTableName(), clonedModelSummary.getTrainingTableName());
        assertEquals(replacedModelSummary.getModelSummaryConfiguration()
                .getString(ProvenancePropertyName.TransformationGroupName, null), TransformationGroup.ALL.getName());

        // Inspect some predictors
        inspectOriginalModelSummaryPredictors(replacedModelSummary);
        log.info("Finished retrieveModelSummaryForReplacedModel");
    }

    @Test(groups = "deployment.lp", dependsOnMethods = "retrieveModelSummaryForClonedModel")
    public void scoreTrainingDataOfClonedModel() throws InterruptedException, IOException {
        log.info("Scoring the training data of the cloned model summary ...");
        System.out.println(String.format("%s/pls/scores/%s/training?useRtsApi=TRUE&performEnrichment=TRUE",
                getRestAPIHostPort(), clonedModelSummary.getId()));
        String applicationId = getRestTemplate().postForObject(
                String.format("%s/pls/scores/%s/training?useRtsApi=TRUE&performEnrichment=TRUE", getRestAPIHostPort(),
                        clonedModelSummary.getId()), //
                null, String.class);
        applicationId = StringUtils.substringBetween(applicationId.split(":")[1], "\"");
        System.out.println(String.format("Score training data applicationId = %s", applicationId));
        assertNotNull(applicationId);
        testJobIsListed("rtsBulkScoreWorkflow", clonedModelSummary.getId(), applicationId);
        log.info("Finished scoreTrainingDataOfClonedModel");
    }

    private void testJobIsListed(final String jobType, final String modelId, String applicationId) {
        boolean any = false;
        while (true) {
            @SuppressWarnings("unchecked")
            List<Object> raw = getRestTemplate()
                    .getForObject(String.format("%s/pls/scores/jobs/%s", getRestAPIHostPort(), modelId), List.class);
            List<Job> jobs = JsonUtils.convertList(raw, Job.class);
            any = Iterables.any(jobs, new Predicate<Job>() {

                @Override
                public boolean apply(@Nullable Job job) {
                    String jobModelId = job.getInputs().get(WorkflowContextConstants.Inputs.MODEL_ID);
                    String jobModelName = job.getInputs().get(WorkflowContextConstants.Inputs.MODEL_DISPLAY_NAME);
                    return job.getJobType() != null && job.getJobType().equals(jobType) && modelId.equals(jobModelId)
                            && modelDisplayName.equals(jobModelName);
                }
            });

            if (any) {
                break;
            }
            sleep(500);
        }

        assertTrue(any);

        JobStatus terminal;
        while (true) {
            Job job = getRestTemplate().getForObject(
                    String.format("%s/pls/jobs/yarnapps/%s", getRestAPIHostPort(), applicationId), Job.class);
            assertNotNull(job);
            if (Job.TERMINAL_JOB_STATUS.contains(job.getJobStatus())) {
                terminal = job.getJobStatus();
                break;
            }
            sleep(1000);
        }
        assertEquals(terminal, JobStatus.COMPLETED);
    }

    private void sleep(long msec) {
        try {
            Thread.sleep(msec);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void saveAttributeSelection(CustomerSpace customerSpace) {
        log.info("Saving attribute selection via internal api ...");
        LeadEnrichmentAttributesOperationMap selectedAttributeMap = checkSelection(customerSpace);
        System.out.println(selectedAttributeMap.getDeselectedAttributes());
        System.out.println(selectedAttributeMap.getSelectedAttributes());
        plsInternalProxy.saveLeadEnrichmentAttributes(customerSpace, selectedAttributeMap);
    }

    private LeadEnrichmentAttributesOperationMap checkSelection(CustomerSpace customerSpace) {
        List<LeadEnrichmentAttribute> enrichmentAttributeList = plsInternalProxy
                .getLeadEnrichmentAttributes(customerSpace, null, null, false);
        LeadEnrichmentAttributesOperationMap selectedAttributeMap = new LeadEnrichmentAttributesOperationMap();
        List<String> selectedAttributes = new ArrayList<>();
        selectedAttributeMap.setSelectedAttributes(selectedAttributes);
        List<String> deselectedAttributes = new ArrayList<>();
        selectedAttributeMap.setDeselectedAttributes(deselectedAttributes);
        int premiumSelectCount = 2;
        int selectCount = 1;

        for (LeadEnrichmentAttribute attr : enrichmentAttributeList) {
            if (attr.getIsPremium()) {
                if (premiumSelectCount > 0) {
                    premiumSelectCount--;
                    selectedAttributes.add(attr.getFieldName());
                }
            } else {
                if (selectCount > 0) {
                    selectCount--;
                    selectedAttributes.add(attr.getFieldName());
                }
            }
        }

        return selectedAttributeMap;
    }

    private ModelSummary waitToDownloadModelSummary(String modelName) throws InterruptedException {
        log.info(String.format("Getting the model whose name contains %s", modelName));
        ModelSummary found = null;
        // Wait for model downloader
        while (true) {
            @SuppressWarnings("unchecked")
            List<Object> summaries = restTemplate.getForObject( //
                    String.format("%s/pls/modelsummaries", getRestAPIHostPort()), List.class);
            for (Object rawSummary : summaries) {
                ModelSummary summary = new ObjectMapper().convertValue(rawSummary, ModelSummary.class);
                if (summary.getName().contains(modelName)) {
                    found = summary;
                }
            }
            if (found != null)
                break;
            Thread.sleep(1000);
        }
        assertNotNull(found);

        @SuppressWarnings("unchecked")
        List<Object> predictors = restTemplate.getForObject(
                String.format("%s/pls/modelsummaries/predictors/all/%s", getRestAPIHostPort(), found.getId()),
                List.class);
        assertTrue(predictors.stream()
                .anyMatch(o -> new ObjectMapper().convertValue(o, Predictor.class).getCategory() != null));

        // Look up the model summary with details
        Object rawSummary = restTemplate.getForObject(
                String.format("%s/pls/modelsummaries/%s", getRestAPIHostPort(), found.getId()), Object.class);
        return JsonUtils.convertValue(rawSummary, ModelSummary.class);
    }

    private void compareRtsScoreWithModeling(ModelSummary modelSummary, int countsForScoring, String tenantId)
            throws IOException, InterruptedException {

        Map<String, ComparedRecord> diffRecords = scoreCompareService.analyzeScores(tenantId,
                RESOURCE_BASE + "/" + fileName, modelSummary.getId(), countsForScoring);
        checkExpectedDifferentCount(diffRecords);
    }

    protected void checkExpectedDifferentCount(Map<String, ComparedRecord> diffRecords) {
        log.info(String.format("diffRecords.size() is %d.", diffRecords.size()));
        String expectedDiffCountStr = System.getProperty("DIFFCOUNT");

        if (StringUtils.isNotBlank(expectedDiffCountStr)) {
            int expectedDiffCount = Integer.valueOf(expectedDiffCountStr);
            log.info("Checking if expected diff count is equal to " + expectedDiffCountStr);
            assertTrue(Math.abs(diffRecords.size() - expectedDiffCount) <= 2, //
                    "Got actual diff: " + diffRecords.size() //
                            +" which is too far away from expected diff count " + expectedDiffCountStr);
        } else {
            log.info("Property DIFFCOUNT not set.");
        }
    }

    protected void inspectOriginalModelSummaryPredictors(ModelSummary modelSummary) {
        // Inspect some predictors
        String rawModelSummary = modelSummary.getDetails().getPayload();
        JsonNode modelSummaryJson = JsonUtils.deserialize(rawModelSummary, JsonNode.class);
        JsonNode predictors = modelSummaryJson.get("Predictors");
        for (int i = 0; i < predictors.size(); ++i) {
            JsonNode predictor = predictors.get(i);
            assertNotEquals(predictor.get("Name"), "Activity_Count_Interesting_Moment_Webinar");
            if (predictor.get("Name") != null && predictor.get("Name").asText() != null) {
                if (predictor.get("Name").asText().equals("Some_Column")) {
                    JsonNode tags = predictor.get("Tags");
                    assertEquals(tags.size(), 1);
                    assertEquals(tags.get(0).textValue(), ModelingMetadata.INTERNAL_TAG);
                    // as per requirement, we moved attributes from
                    // ModelingMetadata.CATEGORY_LEAD_INFORMATION to
                    // Category.ACCOUNT_ATTRIBUTES
                    assertEquals(Category.fromName(predictor.get("Category").textValue()), Category.ACCOUNT_ATTRIBUTES);
                } else if (predictor.get("Name").asText().equals("Industry")) {
                    JsonNode approvedUsages = predictor.get("ApprovedUsage");
                    assertEquals(approvedUsages.size(), 1);
                    assertEquals(approvedUsages.get(0).textValue(), ApprovedUsage.MODEL_ALLINSIGHTS.toString());
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void assertJobExistsWithModelIdAndModelName(final String jobModelId) {
        log.info(String.format("The model_id is: %s", jobModelId));
        List<Object> rawJobs = restTemplate.getForObject(String.format("%s/pls/jobs", getRestAPIHostPort()),
                List.class);
        List<Job> jobs = JsonUtils.convertList(rawJobs, Job.class);
        String jobsInString = "There are " + rawJobs.size() + " jobs:\n";
        try {
            jobsInString += new ObjectMapper().writerWithDefaultPrettyPrinter()
                    .writeValueAsString(JsonUtils.serialize(rawJobs));
        } catch (IOException e) {
            log.warn(e.getMessage());
        }
        assertTrue(Iterables.any(jobs, new Predicate<Job>() {
            @Override
            public boolean apply(@Nullable Job job) {
                assertNotNull(job.getId());
                job = restTemplate.getForObject(
                        String.format("%s/pls/jobs/%s", getRestAPIHostPort(), Long.toString(job.getId())), Job.class);
                return job != null && job.getOutputs() != null
                        && jobModelId.equals(job.getOutputs().get(WorkflowContextConstants.Inputs.MODEL_ID))
                        && modelDisplayName
                                .equals(job.getInputs().get(WorkflowContextConstants.Inputs.MODEL_DISPLAY_NAME));
            }
        }), jobsInString);

        List<String> workflowIds = jobs.stream().map(job -> job.getId().toString()).collect(Collectors.toList());
        StringBuilder sb = new StringBuilder();
        sb.append("?");
        for (String jobId : workflowIds) {
            sb.append(String.format("jobId=%s&", jobId));
        }
        String urlSuffix = sb.substring(0, sb.length() - 1).toString();
        rawJobs = restTemplate.getForObject(String.format("%s/pls/jobs%s", getRestAPIHostPort(), urlSuffix),
                List.class);
        // FIXME: (Yintao - M21) remove this assertion temporarily to unblock
        // M21 release
        // assertEquals(CollectionUtils.size(rawJobs),
        // CollectionUtils.size(jobs));
        jobs = JsonUtils.convertList(rawJobs, Job.class);
        log.info(String.format("Jobs are %s", jobs));
    }

    public String prepareModel(SchemaInterpretation schemaInterpretation, String fileName) throws InterruptedException {
        if (!StringUtils.isBlank(fileName)) {
            this.fileName = fileName;
        }
        if (schemaInterpretation != null) {
            this.schemaInterpretation = schemaInterpretation;
        }

        log.info("Uploading File");
        uploadFile();
        sourceFile = getSourceFile();
        log.info(sourceFile.getName());
        log.info("Resolving Metadata");
        resolveMetadata();
        log.info("Creating Model");
        createModel();
        retrieveModelSummary();
        ModelSummary modelSummary = getModelSummary();
        String modelId = modelSummary.getId();
        log.info("modeling id: " + modelId);
        activateModelSummary(modelId);
        return modelId;
    }

    public SourceFile getSourceFile() {
        return sourceFile;
    }

    public RestTemplate getRestTemplate() {
        return restTemplate;
    }

    public Tenant getFirstTenant() {
        return firstTenant;
    }

    public Tenant getSecondTenant() {
        return secondTenant;
    }

    public ModelSummary getModelSummary() {
        return originalModelSummary;
    }

}
