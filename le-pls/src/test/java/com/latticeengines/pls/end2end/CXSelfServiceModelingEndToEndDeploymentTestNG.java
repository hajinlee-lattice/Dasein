package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTime;
import org.python.modules.time.Time;
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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerException;

@Component
public class CXSelfServiceModelingEndToEndDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final String RESOURCE_BASE = "com/latticeengines/pls/end2end/selfServiceModeling/csvfiles";
    private static final Log log = LogFactory.getLog(CXSelfServiceModelingEndToEndDeploymentTestNG.class);

    private Tenant tenantToAttach;
    private String fileName;
    private SchemaInterpretation schemaInterpretation = SchemaInterpretation.SalesforceLead;
    private Connection conn;
    private String sqlServerreport = "10.41.1.97\\sql2012std";
    private String sqlUserName = "dataloader_user";
    private String sqlPassword = "password";
    private String sqlDBName = "MQTest";

    private String webhdfsnn1 = "http://10.41.1.185:50070/explorer.html#/";
    private String webhdfsnn2 = "http://10.41.1.104:50070/explorer.html#/";

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @Value("${pls.fs.defaultFS}")
    private String plsHdfsPath;

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private Configuration yarnConfiguration;

    HashMap<String, Double> model_score_map = new HashMap<String, Double>();

    @BeforeClass(groups = "qa.lp")
    public void setup() throws Exception {
        fileName = System.getenv("FILENAME");
        log.info("WSHOME:" + System.getenv("WSHOME"));
        log.info("Bootstrapping test tenants using tenant console ...");
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
        tenantToAttach = testBed.getMainTestTenant();
        log.info(String.format("Test environment setup finished. tenant name: %s ", tenantToAttach.getName()));
        log.info("start to connect DB...");
        conn = dbConnect();
    }

    @AfterClass(groups = "qa.lp")
    public void tearDown() throws Exception {
        log.info("This is tear down method...");
        if (conn != null) {
            log.info("Close sql connection...");
            conn.close();
        }
    }

    @Test(groups = "qa.lp", enabled = true)
    public void testModelQuality() throws InterruptedException, IOException, SQLException, ParseException, ExecutionException {
        String fullPath = System.getenv("WSHOME") + "/le-pls/src/test/resources/" + RESOURCE_BASE + '/' + fileName;
        log.info("Folder Full Path:" + fullPath);
        File folder = new File(fullPath);
        if (folder.isFile()) {
            if (fileName.toLowerCase().endsWith(".csv")) {
                processOneFile(fileName);
            } else {
                log.info(String.format("File %s needs to be have .csv extension for modeling", fileName));
            }
        } else if (folder.isDirectory()) {
            File[] listOfFiles = folder.listFiles();
            String FolderName = fileName;
            log.info(String.format("========== there are %d files", listOfFiles.length));
            int taskSize = 0;
            ArrayList<String> fileNames = new ArrayList<String>();
            for (int i = 0; i < listOfFiles.length; i++) {
                if (listOfFiles[i].getName().toLowerCase().endsWith(".csv")) {
                    fileNames.add(FolderName + '/' + listOfFiles[i].getName());
                    taskSize++;
                } else {
                    log.info(String.format("File %s in folder %s needs to be have .csv extension for modeling", listOfFiles[i].getName(), folder.getPath()));
                }
            }
            ExecutorService pool = Executors.newFixedThreadPool(taskSize);
            List<Future<String>> list = new ArrayList<>();
            for (int i = 0; i < taskSize; i++) {
                log.info(">>>>>>>>>>>start one thread");
                Callable<String> c = new MyCallable(fileNames.get(i));
                Future<String> f = pool.submit(c);
                list.add(f);
                Time.sleep(60);
            }
            pool.shutdown();
            pool.awaitTermination(300, TimeUnit.MINUTES);
            for (Future<String> f : list) {
                log.info(">>>>>>>>> " + f.get().toString());
            }
        }
    }

    public boolean processOneFile(String csvFile) throws InterruptedException, IOException, SQLException, ParseException {
        boolean result = false;
        log.info(String.format("======CSV File Name is: %s=========", csvFile));
        String modelId = prepareModel(SchemaInterpretation.SalesforceLead, csvFile);
        downloadModels();
        Double rocScore = model_score_map.get(modelId);
        WriteQualityTestReport(csvFile, rocScore);
        result = true;
        return result;
    }

    public void downloadModels() throws IOException {
        String tenantName = tenantToAttach.getId();
        String hdfsPath = String.format("%s/%s/%s", plsHdfsPath, modelingServiceHdfsBaseDir, tenantName);
        // TODO: do we make local path a parameter from Jenkins?
        try {
            String localPath = System.getenv("MODEL_LOCALPATH");
            if (HdfsUtils.fileExists(yarnConfiguration, hdfsPath)) {
                // remove local copy first
                FileUtils.deleteDirectory(new File(localPath, tenantName));
                HdfsUtils.copyHdfsToLocal(yarnConfiguration, hdfsPath, localPath + "/" + tenantName);
                log.info(String.format("File %s is copied to local machine at %s", hdfsPath, localPath));
            } else {
                log.info(String.format("File %s does not exist on HDFS", hdfsPath));
            }
        } catch (IOException ioe) {
            log.info("ERROR: " + ioe.getMessage());
        }
    }

    @SuppressWarnings("rawtypes")
    public SourceFile uploadFile(String csvFileName) {
        if (schemaInterpretation == null) {
            schemaInterpretation = SchemaInterpretation.SalesforceLead;
        }
        LinkedMultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
        map.add("file", new ClassPathResource(RESOURCE_BASE + "/" + csvFileName));
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);
        String displayName = csvFileName;
        if (csvFileName.indexOf('/') >= 0) {
            displayName = csvFileName.split("/")[1];
        }
        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<>(map, headers);
        
        ResponseDocument response = restTemplate.postForObject( //
                String.format("%s/pls/models/uploadfile/unnamed?displayName=%s", getRestAPIHostPort(),
                        displayName), //
                requestEntity, ResponseDocument.class);
        SourceFile sourceFile = new ObjectMapper().convertValue(response.getResult(), SourceFile.class);
        log.info(sourceFile.getName());
        
        map = new LinkedMultiValueMap<>();
        map.add("metadataFile", new ClassPathResource(
                "com/latticeengines/pls/end2end/selfServiceModeling/pivotmappingfiles/pivotvalues.txt"));
        headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        requestEntity = new HttpEntity<>(map, headers);
        
        String pivotvalues="pivotvalues_"+displayName.substring(0, displayName.lastIndexOf("."));

        response = restTemplate.postForObject( //
                String.format("%s/pls/metadatauploads/modules/%s/%s?artifactName=%s", getRestAPIHostPort(), "module1",
                        "pivotmappings", pivotvalues), //
                requestEntity, ResponseDocument.class);
        String pivotFilePath = new ObjectMapper().convertValue(response.getResult(), String.class);
        log.info(pivotFilePath);
        
        return sourceFile;
    }

    @SuppressWarnings("rawtypes")
    public void resolveMetadata(SourceFile sourceFile) {
        sourceFile.setSchemaInterpretation(schemaInterpretation);
        ResponseDocument response = restTemplate.getForObject(
                String.format("%s/pls/models/uploadfile/%s/fieldmappings?schema=%s", getRestAPIHostPort(),
                        sourceFile.getName(), schemaInterpretation.name()), ResponseDocument.class);
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
        restTemplate.postForObject(
                String.format("%s/pls/models/uploadfile/fieldmappings?displayName=%s", getRestAPIHostPort(),
                        sourceFile.getName()), mappings, Void.class);
    }

    public String createModel(SourceFile sourceFile, String csvFileName) {
        String customer="";
        String csvFile = csvFileName;
        if (csvFileName.indexOf('/') >= 0) {
            csvFile = csvFileName.split("/")[1];
        }
        if (csvFile.lastIndexOf(".") > 0) {
            customer = csvFile.substring(0, csvFile.lastIndexOf("."));
        }

        ModelingParameters parameters = new ModelingParameters();
        parameters.setName("CXSelfServiceModelingEndToEndDeploymentTestNG_" +customer+"_"+ DateTime.now().getMillis());
        parameters.setDescription("Test");
        parameters.setFilename(sourceFile.getName());
        parameters.setModuleName("module1");
        parameters.setPivotFileName("pivotvalues_"+customer+".csv");
        String modelName = parameters.getName();
        model(parameters);
        return modelName;
    }

    @SuppressWarnings("rawtypes")
    private String model(ModelingParameters parameters) {
        ResponseDocument response;
        response = restTemplate.postForObject(
                String.format("%s/pls/models/%s", getRestAPIHostPort(), parameters.getName()), parameters,
                ResponseDocument.class);
        String modelingWorkflowApplicationId = new ObjectMapper().convertValue(response.getResult(), String.class);
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
        return modelingWorkflowApplicationId;
    }

    public void retrieveReport(String modelingWorkflowApplicationId) {
        Job job = restTemplate.getForObject( //
                String.format("%s/pls/jobs/yarnapps/%s", getRestAPIHostPort(), modelingWorkflowApplicationId), //
                Job.class);
        assertNotNull(job);
        List<Report> reports = job.getReports();
        assertEquals(reports.size(), 2);
    }

    public ModelSummary retrieveModelSummary(String modelName) throws InterruptedException {
        ModelSummary originalModelSummary = getModelSummary(modelName);
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
        return originalModelSummary;
    }

    public void retrieveErrorsFile(SourceFile sourceFile) {
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

    private void WriteQualityTestReport(String csvFileName, Double rocScore) throws SQLException, ParseException {

        String tenantName = tenantToAttach.getId();
        String hdfsPath_nn1 = String.format("%s%s%s", webhdfsnn1, modelingServiceHdfsBaseDir, tenantName);
        String hdfsPath_nn2 = String.format("%s%s%s", webhdfsnn2, modelingServiceHdfsBaseDir, tenantName);
        String customer = "";
        String csvFile = csvFileName;
        if (csvFileName.indexOf('/') >= 0) {
            csvFile = csvFileName.split("/")[1];
        }
        if (csvFile.lastIndexOf(".") > 0) {
            customer = csvFile.substring(0, csvFile.lastIndexOf("."));
        }
        if (customer.indexOf('/') >= 0) {
            customer = customer.split("/")[1];
        }
        if (customer.indexOf("_LP3_") > 0) {
            customer = customer.split("_LP3_")[0];
        } else if (customer.split("_").length > 2) {
            customer = customer.split("_")[0] + '_' + customer.split("_")[1];
        }
        System.out.println("Connected to server !!!");
        Statement statement = conn.createStatement();
        log.info("start to insert into ModelQualityReport db...");
        int res = statement.executeUpdate("insert INTO [dbo].[ModelQualityReport] ([Customer],[CSVFile],[ModelArtifacts_NN1],[ModelArtifacts_NN2],[ROC],[CreatedDate]) VALUES ('" + customer + "', '" + csvFile + "','" + hdfsPath_nn1 + "','" + hdfsPath_nn2 + "'," + rocScore + ",'" + getCurrentTimeStamp() + "')");
        assertEquals(res, 1);
        log.info("completed to insert DB...");
        statement.close();
    }

    private String getCurrentTimeStamp() throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat(
                "yyyy-MM-dd HH:mm:ss");
        java.util.Date today = new java.util.Date();
        return dateFormat.format(today.getTime());
    }

    private Connection dbConnect() throws SQLServerException {
        SQLServerDataSource dataSource = new SQLServerDataSource();
        dataSource.setServerName(sqlServerreport);
        dataSource.setPortNumber(1433);
        dataSource.setDatabaseName(sqlDBName);
        dataSource.setUser(sqlUserName);
        dataSource.setPassword(sqlPassword);
        Connection conn = dataSource.getConnection();
        return conn;
    }

    public String prepareModel(SchemaInterpretation schemaInterpretation, String fileName)
            throws InterruptedException {
        String csvFileName = fileName;
        if (!StringUtils.isBlank(fileName)) {
            this.fileName = fileName;
        }
        if (schemaInterpretation != null) {
            this.schemaInterpretation = schemaInterpretation;
        }
        log.info("Uploading File");
        SourceFile sourceFile = uploadFile(csvFileName);
        log.info(sourceFile.getName());
        log.info("Resolving Metadata");
        resolveMetadata(sourceFile);
        log.info("Creating Model");
        String modelName = createModel(sourceFile,csvFileName);
        ModelSummary modelSummary = retrieveModelSummary(modelName);
        String modelId = modelSummary.getId();
        log.info("modeling id: " + modelId);
        Double rocScore = modelSummary.getRocScore();
        log.info(String.format(">>>>>>>>>>>>>>>> modeling iD is:%s and rocScore is: %f", modelId, rocScore));
        model_score_map.put(modelId, rocScore);
        return modelId;
    }

    public RestTemplate getRestTemplate() {
        return restTemplate;
    }

    public Tenant getTenant() {
        return tenantToAttach;
    }

    class MyCallable implements Callable<String> {
        private String CSVFileName;

        MyCallable(String csvfileName) {
            this.CSVFileName = csvfileName;
        }
        @Override
        public String call() throws Exception {
            String result = "";
            System.out.println(">>>>>>>>" + CSVFileName + " start to process!");
            boolean result_process = processOneFile(CSVFileName);
            if (result_process) {
                result = "======" + CSVFileName + "Have been processed successfully!";
            } else {
                result = "!!!!!!!!" + CSVFileName + "processed Failed!";
            }
            return result;
        }
    }
}