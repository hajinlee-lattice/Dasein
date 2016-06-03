package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

import javax.annotation.Nullable;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.monitor.metric.service.impl.SplunkLogMetricWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.pls.CloneModelingParameters;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.VdbMetadataField;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.latticeengines.monitor.metric.service.impl.SplunkLogMetricWriter;

@Component
public class CXSelfServiceModelingEndToEndDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final String RESOURCE_BASE = "com/latticeengines/pls/end2end/selfServiceModeling/csvfiles";
    private static final Log log = LogFactory.getLog(CXSelfServiceModelingEndToEndDeploymentTestNG.class);

    @Autowired
    private WorkflowProxy workflowProxy;
    private Tenant tenantToAttach;
    private SourceFile sourceFile;
    private String modelingWorkflowApplicationId;
    private String modelName;
    private ModelSummary originalModelSummary;
    private String fileName;
    private SchemaInterpretation schemaInterpretation = SchemaInterpretation.SalesforceLead;
    private Function<List<LinkedHashMap<String, String>>, Void> unknownColumnHandler;

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @Value("${pls.fs.defaultFS}")
    private String plsHdfsPath;

    @Autowired
    private Configuration yarnConfiguration;

    Double rocScore;

    @BeforeClass(groups = "qa.lp")
    public void setup() throws Exception {
        fileName = System.getenv("FILENAME");
        log.info("WSHOME:" + System.getenv("WSHOME"));
        log.info("Bootstrapping test tenants using tenant console ...");
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
        tenantToAttach = testBed.getMainTestTenant();
        log.info(String.format("Test environment setup finished. tenant name: %s ", tenantToAttach.getName()));

        //fileName = "Hootsuite_PLS132_LP3_ScoringLead_20160330_165806_modified.csv";

    }

    @Test(groups="qa.lp", enabled=true)
    public void testModelQuality() throws InterruptedException, IOException
    {
        String fullPath=System.getenv("WSHOME")+"/le-pls/src/test/resources/"+RESOURCE_BASE+'/'+fileName;
        log.info("Folder Full Path:"+fullPath);
        File folder = new File(fullPath);
        if (folder.isFile())
        {
            if (fileName.toLowerCase().endsWith(".csv"))
            {
                //fileName=fileName;
                log.info(String.format("======CSV File Name is: %s=========", fileName));
                String modelId = prepareModel(SchemaInterpretation.SalesforceLead, unknownColumnHandler, fileName);
                downloadModels();
                log.info(String.format("CX: ModelId is %s, ROC score is %f", modelId, rocScore));
              //WriteToSplunk();
            }
            else
            {
                log.info(String.format("The file %s is not CSV file, so skip to model", fileName));
            }
        }
        else if(folder.isDirectory())
        {
            File[] listOfFiles = folder.listFiles();
            String FolderName=fileName;
            log.info(String.format("===========Files numbre is %d", listOfFiles.length));
            for (int i = 0; i < listOfFiles.length; i++)
            {
                if (listOfFiles[i].getName().toLowerCase().endsWith(".csv"))
                {
                    fileName=FolderName+'/'+listOfFiles[i].getName();
                    log.info(String.format("======CSV File Name is: %s=========", fileName));
                    String modelId = prepareModel(SchemaInterpretation.SalesforceLead, unknownColumnHandler, fileName);
                    downloadModels();
                    log.info(String.format("CX: ModelId is %s, ROC score is %f", modelId, rocScore));
                  //WriteToSplunk();
                }
                else
                {
                    log.info(String.format("The file %s in folder %s is not CSV file, so skip to model", listOfFiles[i].getName(),folder.getPath()));
                }
                
                
            }
        }
    }
    
    public void downloadModels() throws IOException {
        String tenantName = tenantToAttach.getId();
        // debug use
        // String tenantName = "LETest1464379257704.LETest1464379257704.Production";
        String hdfsPath = String.format("%s/%s/%s", plsHdfsPath , modelingServiceHdfsBaseDir, tenantName);

        // TODO: do we make local path a parameter from Jenkins?
        try {
            String localPath = System.getenv("MODEL_LOCALPATH");
            if (HdfsUtils.fileExists(yarnConfiguration, hdfsPath)) {
                // remove local copy first
                FileUtils.deleteDirectory(new File(localPath, tenantName));

                HdfsUtils.copyHdfsToLocal(yarnConfiguration, hdfsPath, localPath + "/" + tenantName);
                log.info(String.format("File %s copied to local machine at %s", hdfsPath, localPath));
            } else {
                log.info(String.format("File %s does not exist on HDFS", hdfsPath));
            };
        } catch (IOException ioe) {
            log.info("ERROR: " + ioe.getMessage());
        }

    }



    @SuppressWarnings("rawtypes")
    public void uploadFile() {
        if (schemaInterpretation == null) {
            schemaInterpretation = SchemaInterpretation.SalesforceLead;
        }
        LinkedMultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
        map.add("file", new ClassPathResource(RESOURCE_BASE + "/" + fileName));
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<>(map, headers);
        ResponseDocument response = restTemplate.postForObject( //
                String.format("%s/pls/models/fileuploads/unnamed?schema=%s&displayName=%s", getRestAPIHostPort(),
                        schemaInterpretation, "SelfServiceModeling Test File.csv"), //
                requestEntity, ResponseDocument.class);
        sourceFile = new ObjectMapper().convertValue(response.getResult(), SourceFile.class);
        log.info(sourceFile.getName());
    }

    @SuppressWarnings("rawtypes")
    public void resolveMetadata() {
        ResponseDocument response = restTemplate.getForObject(
                String.format("%s/pls/models/fileuploads/%s/metadata/unknown", getRestAPIHostPort(),
                        sourceFile.getName()), ResponseDocument.class);
        @SuppressWarnings("unchecked")
        List<LinkedHashMap<String, String>> unknownColumns = new ObjectMapper().convertValue(response.getResult(),
                List.class);

        System.out.println("the unknown columsn are: " + unknownColumns);
        log.info("the unknown columsn are: " + unknownColumns);
        if (unknownColumnHandler != null) {
            unknownColumnHandler.apply(unknownColumns);
        }
        response = restTemplate.postForObject(
                String.format("%s/pls/models/fileuploads/%s/metadata/unknown", getRestAPIHostPort(),
                        sourceFile.getName()), unknownColumns, ResponseDocument.class);
    }

    //    @Test(groups = "qa.lp", enabled = true, dependsOnMethods = "resolveMetadata")
    public void createModel() {
        ModelingParameters parameters = new ModelingParameters();
        parameters.setName("CXSelfServiceModelingEndToEndDeploymentTestNG_" + DateTime.now().getMillis());
        parameters.setDescription("Test");
        parameters.setFilename(sourceFile.getName());
        modelName = parameters.getName();
        model(parameters);
    }

    @SuppressWarnings("rawtypes")
    private void model(ModelingParameters parameters) {
        ResponseDocument response;
        response = restTemplate.postForObject(
                String.format("%s/pls/models/%s", getRestAPIHostPort(), parameters.getName()), parameters,
                ResponseDocument.class);
        modelingWorkflowApplicationId = new ObjectMapper().convertValue(response.getResult(), String.class);

        log.info(String.format("Workflow application id is %s", modelingWorkflowApplicationId));
        waitForWorkflowStatus(modelingWorkflowApplicationId, true);

        boolean thrown = false;
        try {
            response = restTemplate.postForObject(
                    String.format("%s/pls/models/%s", getRestAPIHostPort(), UUID.randomUUID()), parameters,
                    ResponseDocument.class);
        } catch (Exception e) {
            thrown = true;
        }

        assertTrue(thrown);

        JobStatus completedStatus = waitForWorkflowStatus(modelingWorkflowApplicationId, false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    //    @Test(groups = "qa.lp", dependsOnMethods = "createModel")
    public void retrieveReport() {
        Job job = restTemplate.getForObject( //
                String.format("%s/pls/jobs/yarnapps/%s", getRestAPIHostPort(), modelingWorkflowApplicationId), //
                Job.class);
        assertNotNull(job);
        List<Report> reports = job.getReports();
        assertEquals(reports.size(), 2);
    }

    public void retrieveModelSummary() throws InterruptedException {
        originalModelSummary = getModelSummary(modelName);
        assertNotNull(originalModelSummary);
        assertEquals(originalModelSummary.getSourceSchemaInterpretation(),
                SchemaInterpretation.SalesforceLead.toString());
        assertNotNull(originalModelSummary.getTrainingTableName());
        assertFalse(originalModelSummary.getTrainingTableName().isEmpty());
        // Inspect some predictors
        String rawModelSummary = originalModelSummary.getDetails().getPayload();
        JsonNode modelSummaryJson = JsonUtils.deserialize(rawModelSummary, JsonNode.class);
        JsonNode predictors = modelSummaryJson.get("Predictors");
        for (int i = 0; i < predictors.size(); ++i) {
            JsonNode predictor = predictors.get(i);
            if (predictor.get("Name") != null && predictor.get("Name").asText() != null
                    && predictor.get("Name").asText().equals("Some_Column")) {
                JsonNode tags = predictor.get("Tags");
                assertEquals(tags.size(), 1);
                assertEquals(tags.get(0).textValue(), ModelingMetadata.INTERNAL_TAG);
                assertEquals(predictor.get("Category").textValue(), ModelingMetadata.CATEGORY_LEAD_INFORMATION);
            }
            // TODO Assert more
        }
    }

    public void retrieveErrorsFile() {
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
    }

    private ModelSummary getModelSummary(String modelName) throws InterruptedException {
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
        assertTrue(Iterables.any(predictors, new Predicate<Object>() {

            @Override
            public boolean apply(@Nullable Object raw) {
                Predictor predictor = new ObjectMapper().convertValue(raw, Predictor.class);
                return predictor.getCategory() != null;
            }
        }));

        // Look up the model summary with details
        Object rawSummary = restTemplate.getForObject(
                String.format("%s/pls/modelsummaries/%s", getRestAPIHostPort(), found.getId()), Object.class);
        return JsonUtils.convertValue(rawSummary, ModelSummary.class);
    }

    private JobStatus waitForWorkflowStatus(String applicationId, boolean running) {

        int retryOnException = 4;
        Job job = null;

        while (true) {
            try {
                job = workflowProxy.getWorkflowJobFromApplicationId(applicationId);
            } catch (Exception e) {
                System.out.println(String.format("Workflow job exception: %s", e.getMessage()));

                job = null;
                if (--retryOnException == 0)
                    throw new RuntimeException(e);
            }

            if ((job != null) && ((running && job.isRunning()) || (!running && !job.isRunning()))) {
                return job.getJobStatus();
            }

            try {
                Thread.sleep(30000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public String prepareModel(SchemaInterpretation schemaInterpretation,
                               Function<List<LinkedHashMap<String, String>>, Void> unknownColumnHandler, String fileName)
            throws InterruptedException {
        if (!StringUtils.isBlank(fileName)) {
            this.fileName = fileName;
        }
        if (schemaInterpretation != null) {
            this.schemaInterpretation = schemaInterpretation;
        }
        if (unknownColumnHandler != null) {
            this.unknownColumnHandler = unknownColumnHandler;
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
        rocScore = modelSummary.getRocScore();
        return modelId;
    }

    public SourceFile getSourceFile() {
        return sourceFile;
    }

    public RestTemplate getRestTemplate() {
        return restTemplate;
    }

    public Tenant getTenant() {
        return tenantToAttach;
    }

    public ModelSummary getModelSummary() {
        return originalModelSummary;
    }

}
