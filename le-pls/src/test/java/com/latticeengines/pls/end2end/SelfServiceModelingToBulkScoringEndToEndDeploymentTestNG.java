package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.util.LinkedMultiValueMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class SelfServiceModelingToBulkScoringEndToEndDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final String RESOURCE_BASE = "com/latticeengines/pls/end2end/selfServiceModeling/csvfiles";
    private static final Logger log = LoggerFactory.getLogger(SelfServiceModelingEndToEndDeploymentTestNG.class);

    private static final int TOTAL_TRAINING_LINES = 1126;

    private static final int TOTAL_TESTING_LINES = 1127;

    private static final String TRAINING_CSV_FILE = "Lattice_Relaunch_Small.csv";

    private static final String TESTING_CSV_FILE = "Lattice_Relaunch_Small.csv";

    private static final String TESTING_FILE_DISPLAY_NAME = "SelfServiceScoring Test File.csv";

    private static final double DIFFERENCE_THRESHOLD = 1;

    private Path mrScoreResultDir;

    private Path rtsScoreReusltDir;

    @Autowired
    private SelfServiceModelingEndToEndDeploymentTestNG selfServiceModeling;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ScoreCorrectnessService scoreCompareService;

    private String modelId;
    private String applicationId;
    private Long jobId;

    private String trainingFileName;
    private String testingFileName;

    private SourceFile sourceFile;

    private String jobType;

    private Tenant tenant;

    @BeforeClass(groups = "deployment.lp")
    public void setup() throws Exception {
        selfServiceModeling.setup();
        trainingFileName = TRAINING_CSV_FILE;
        testingFileName = TESTING_CSV_FILE;
        tenant = selfServiceModeling.getFirstTenant();
        modelId = selfServiceModeling.prepareModel(SchemaInterpretation.SalesforceLead, trainingFileName);
    }

    @Test(groups = "deployment.lp", enabled = true, timeOut = 1200000)
    public void testScoreTrainingDataUsingMR() throws Exception {
        System.out.println(String.format("%s/pls/scores/%s/training", getRestAPIHostPort(), modelId));
        applicationId = selfServiceModeling.getRestTemplate().postForObject(
                String.format("%s/pls/scores/%s/training", getRestAPIHostPort(), modelId), //
                null, String.class);
        applicationId = StringUtils.substringBetween(applicationId.split(":")[1], "\"");
        System.out.println(String.format("Score training data applicationId = %s", applicationId));
        assertNotNull(applicationId);
        jobType = "scoreWorkflow";
        testScoreMRTrainingData();
    }

    @Test(groups = "deployment.lp", dependsOnMethods = "testScoreTrainingDataUsingMR", enabled = true, timeOut = 2400000)
    public void testScoreTrainingDataUsingRTS() throws Exception {
        System.out.println(String.format("%s/pls/scores/%s/training?useRtsApi=TRUE&performEnrichment=TRUE&debug=TRUE",
                getRestAPIHostPort(), modelId));
        applicationId = selfServiceModeling.getRestTemplate().postForObject(
                String.format("%s/pls/scores/%s/training?useRtsApi=TRUE&performEnrichment=TRUE&debug=TRUE",
                        getRestAPIHostPort(), modelId), //
                null, String.class);
        applicationId = StringUtils.substringBetween(applicationId.split(":")[1], "\"");
        System.out.println(String.format("Score training data applicationId = %s", applicationId));
        assertNotNull(applicationId);
        jobType = "rtsBulkScoreWorkflow";
        testScoreRTSTrainingData();
    }

    @Test(groups = "deployment.lp", dependsOnMethods = "testScoreTrainingDataUsingRTS", enabled = true, timeOut = 1200000)
    public void testScoreTestingDataUsingMR() throws Exception {
        uploadTestingDataFile();
        resolveMetadata();
        System.out.println(String.format("%s/pls/scores/%s?fileName=%s", getRestAPIHostPort(), modelId,
                sourceFile.getName()));
        applicationId = selfServiceModeling.getRestTemplate().postForObject(
                String.format("%s/pls/scores/%s?fileName=%s", getRestAPIHostPort(), modelId, sourceFile.getName()), //
                null, String.class);
        applicationId = StringUtils.substringBetween(applicationId.split(":")[1], "\"");
        System.out.println(String.format("Score testing data applicationId = %s", applicationId));
        assertNotNull(applicationId);
        jobType = "importMatchAndScoreWorkflow";
        testScoreMRTestingData();
    }

    @Test(groups = "deployment.lp", dependsOnMethods = "testScoreTestingDataUsingMR", enabled = true, timeOut = 1200000)
    public void testScoreTestingDataUsingRTS() throws Exception {
        uploadTestingDataFile();
        resolveMetadata();
        System.out.println(String.format(
                "%s/pls/scores/%s?fileName=%s&useRtsApi=TRUE&performEnrichment=TRUE&debug=TRUE", getRestAPIHostPort(),
                modelId, sourceFile.getName()));
        applicationId = selfServiceModeling.getRestTemplate().postForObject(
                String.format("%s/pls/scores/%s?fileName=%s&useRtsApi=TRUE&performEnrichment=TRUE&debug=TRUE",
                        getRestAPIHostPort(), modelId, sourceFile.getName()), //
                null, String.class);
        applicationId = StringUtils.substringBetween(applicationId.split(":")[1], "\"");
        System.out.println(String.format("Score testing data applicationId = %s", applicationId));
        assertNotNull(applicationId);
        jobType = "importAndRTSBulkScoreWorkflow";
        testScoreRTSTestingData();
    }

    private void testScoreMRTrainingData() throws IOException {
        testJobIsListed(jobType);
        poll();
        downloadCsv(TOTAL_TRAINING_LINES);
    }

    private void testScoreRTSTrainingData() throws IOException {
        testJobIsListed(jobType);
        poll();
        downloadCsv(TOTAL_TRAINING_LINES);
        retrieveErrorsFile(true);
        scoreCompareService.analyzeScores(tenant.getId(), RESOURCE_BASE + "/" + TRAINING_CSV_FILE, modelId,
                (int) (TOTAL_TRAINING_LINES * 0.2));
        compareDataResult();
    }

    private void compareDataResult() throws IOException {
        Map<String, ScoreResult> mrResults = getScoreResults(false);
        Map<String, ScoreResult> rtsResults = getScoreResults(true);
        if (!compareScoringResults(mrResults, rtsResults)) {
            assertTrue(false, "MR score and RTS score are not the same within safty range.");
        }
        cleanResults();
    }

    private Map<String, ScoreResult> getScoreResults(boolean getRtsResult) throws IOException {
        Map<String, ScoreResult> scoreResults = new HashMap<String, ScoreResult>();
        final String scoreDirPrefix = getRtsResult ? "/RTSBulkScoreResult_" : "/ScoreResult_";
        System.out.println("getRtsResult is " + getRtsResult + ", and scoreDirPrefix is " + scoreDirPrefix);

        List<String> scoreDir = HdfsUtils.getFilesForDirRecursive(
                yarnConfiguration,
                PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId().toString(),
                        CustomerSpace.parse(tenant.getName())).toString(), new HdfsFileFilter() {
                    @Override
                    public boolean accept(FileStatus file) {
                        if (!file.isDirectory() && file.getPath().toString().contains(scoreDirPrefix)
                                && file.getPath().getName().endsWith("avro")) {
                            return true;
                        }
                        return false;
                    }
                }, true);

        System.out.println(scoreDir);
        Path rtsScoreReusltPath = new Path(scoreDir.get(0));
        if (getRtsResult) {
            rtsScoreReusltDir = rtsScoreReusltPath.getParent();
        } else {
            mrScoreResultDir = rtsScoreReusltPath.getParent();
        }

        List<GenericRecord> scores = AvroUtils.getData(yarnConfiguration, scoreDir);
        for (GenericRecord score : scores) {
            String id = String.valueOf(score.get(InterfaceName.Id.toString()));
            Double rawScore = score.get(ScoreResultField.RawScore.displayName) != null ? Double.valueOf(score.get(
                    ScoreResultField.RawScore.displayName).toString()) : null;
            if (score.get(ScoreResultField.Percentile.displayName) != null) {
                Double percentileScore = Double.valueOf(score.get(ScoreResultField.Percentile.displayName).toString());
                ScoreResult scoreResult = new ScoreResult(rawScore, percentileScore);
                scoreResults.put(id, scoreResult);
            }
        }
        return scoreResults;
    }

    private boolean compareScoringResults(Map<String, ScoreResult> truthResults,
            Map<String, ScoreResult> comparingResults) {
        int differentResultNum = 0;
        for (String key : truthResults.keySet()) {
            if (!truthResults.get(key).equals(comparingResults.get(key))) {
                System.err.println(String.format("For Id: %s, the MR score is %s, and rts score is %s.", key,
                        truthResults.get(key), comparingResults.get(key)));
                differentResultNum++;
            }
        }
        System.out.println("differentResultNum is " + differentResultNum);
        double differentRate = (double) (differentResultNum / (double) truthResults.size() * 100.0);
        System.out.println(String.format("differentRate is %4.6f percent", differentRate));
        return differentRate < DIFFERENCE_THRESHOLD;
    }

    private void cleanResults() throws IOException {
        HdfsUtils.rmdir(yarnConfiguration, rtsScoreReusltDir.toString());
        HdfsUtils.rmdir(yarnConfiguration, mrScoreResultDir.toString());
    }

    private void testScoreMRTestingData() throws IOException {
        testJobIsListed(jobType);
        poll();
        downloadCsv(TOTAL_TESTING_LINES);
        retrieveErrorsFile(false);
    }

    private void testScoreRTSTestingData() throws IOException {
        testJobIsListed(jobType);
        poll();
        downloadCsv(TOTAL_TESTING_LINES);
        retrieveErrorsFile(false);
        compareDataResult();
    }

    private void retrieveErrorsFile(boolean errorsExpected) {
        // Relies on error in Account.csv
        restTemplate.getMessageConverters().add(new ByteArrayHttpMessageConverter());
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Arrays.asList(MediaType.ALL));
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<byte[]> response = restTemplate.exchange(
                String.format("%s/pls/scores/jobs/%s/errors", getRestAPIHostPort(), jobId), HttpMethod.GET, entity,
                byte[].class);
        assertEquals(response.getStatusCode(), HttpStatus.OK);
        if (errorsExpected) {
            String errors = new String(response.getBody());
            assertTrue(errors.length() > 0);
        }
    }

    @SuppressWarnings("rawtypes")
    private void uploadTestingDataFile() {
        LinkedMultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
        map.add("file", new ClassPathResource(RESOURCE_BASE + "/" + testingFileName));
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<>(map, headers);
        ResponseDocument response = selfServiceModeling.getRestTemplate().postForObject( //
                String.format("%s/pls/scores/fileuploads?modelId=%s&displayName=%s", getRestAPIHostPort(), modelId,
                        TESTING_FILE_DISPLAY_NAME), requestEntity, ResponseDocument.class);
        assertTrue(response.isSuccess());
        sourceFile = new ObjectMapper().convertValue(response.getResult(), SourceFile.class);
        log.info(sourceFile.getName());
    }

    @SuppressWarnings("rawtypes")
    private void resolveMetadata() {
        ResponseDocument response = restTemplate.postForObject(String.format(
                "%s/pls/scores/fileuploads/fieldmappings?modelId=%s&csvFileName=%s", getRestAPIHostPort(), modelId,
                sourceFile.getName()), null, ResponseDocument.class);

        FieldMappingDocument fieldMappingDocument = new ObjectMapper().convertValue(response.getResult(),
                FieldMappingDocument.class);
        List<FieldMapping> fieldMappings = new ArrayList<>();
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getUserField() != null) {
                fieldMappings.add(fieldMapping);
            }
        }
        fieldMappingDocument.setFieldMappings(fieldMappings);
        assertRequiredFieldsMapped(fieldMappings, fieldMappingDocument.getRequiredFields());

        restTemplate.postForObject(String.format(
                "%s/pls/scores/fileuploads/fieldmappings/resolve?modelId=%s&csvFileName=%s", getRestAPIHostPort(),
                modelId, sourceFile.getName()), fieldMappingDocument, Void.class);
    }

    @SuppressWarnings({ "unchecked", "unused" })
    private void assertAllHeaderFieldsIncludedInFieldMappings(List<FieldMapping> fieldMappings) {
        List<String> fileHeaders = selfServiceModeling.getRestTemplate().getForObject(
                String.format("%s/pls/scores/fileuploads/headerfields?csvFileName=%s", getRestAPIHostPort(),
                        sourceFile.getName()), List.class);
        for (FieldMapping fieldMapping : fieldMappings) {
            assertTrue(fileHeaders.contains(fieldMapping.getUserField()));
            fileHeaders.remove(fieldMapping.getUserField());
        }

        assertTrue(fileHeaders.isEmpty());
    }

    private void assertRequiredFieldsMapped(List<FieldMapping> fieldMappings, List<String> requiredFields) {
        for (String requiredField : requiredFields) {
            boolean foundRequiredField = false;
            for (FieldMapping fieldMapping : fieldMappings) {
                if (fieldMapping.getMappedField() != null && fieldMapping.getMappedField().equals(requiredField)) {
                    foundRequiredField = true;
                    assertNotNull(fieldMapping.getUserField());
                    assertTrue(fieldMapping.isMappedToLatticeField());
                }
            }
            assertTrue(foundRequiredField);
        }
    }

    private void downloadCsv(int totalRowNumber) throws IOException {
        selfServiceModeling.getRestTemplate().getMessageConverters().add(new ByteArrayHttpMessageConverter());
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Arrays.asList(MediaType.ALL));
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<byte[]> response = selfServiceModeling.getRestTemplate().exchange(
                String.format("%s/pls/scores/jobs/%d/results/score", getRestAPIHostPort(), jobId), HttpMethod.GET,
                entity, byte[].class);
        assertEquals(response.getStatusCode(), HttpStatus.OK);
        String results = new String(response.getBody());
        assertTrue(response.getHeaders().getFirst("Content-Disposition").contains("_scored.csv"));
        assertTrue(results.length() > 0);
        CSVParser parser = null;
        InputStream is = new ByteArrayInputStream(response.getBody());
        InputStreamReader reader = new InputStreamReader(is);
        CSVFormat format = LECSVFormat.format;
        try {
            parser = new CSVParser(reader, format);
            Set<String> csvHeaders = parser.getHeaderMap().keySet();
            assertTrue(csvHeaders.contains("Activity_Count_Click_Email"));
            assertTrue(csvHeaders.contains("Industry"));
            assertTrue(csvHeaders.contains("PhoneNumber"));
            assertTrue(csvHeaders.contains("Score"));
            assertTrue(csvHeaders.contains("Rating"));
            assertFalse(csvHeaders.contains("RawScore"));

            int line = 1;
            for (CSVRecord record : parser.getRecords()) {
                if (!record.get("Id").contains("error line")) {
                    assertTrue(StringUtils.isNotEmpty(record.get("Score")));
                }
                line++;
            }
            assertEquals(line, totalRowNumber);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            parser.close();
        }
    }

    private void poll() {
        JobStatus terminal;
        while (true) {
            Job job = selfServiceModeling.getRestTemplate().getForObject(
                    String.format("%s/pls/jobs/yarnapps/%s", getRestAPIHostPort(), applicationId), Job.class);
            assertNotNull(job);
            jobId = job.getId();
            if (Job.TERMINAL_JOB_STATUS.contains(job.getJobStatus())) {
                terminal = job.getJobStatus();
                break;
            }
            sleep(1000);
        }

        assertEquals(terminal, JobStatus.COMPLETED);
    }

    private void testJobIsListed(final String jobType) {
        boolean any = false;
        while (true) {
            @SuppressWarnings("unchecked")
            List<Object> raw = selfServiceModeling.getRestTemplate().getForObject(
                    String.format("%s/pls/scores/jobs/%s", getRestAPIHostPort(), modelId), List.class);
            List<Job> jobs = JsonUtils.convertList(raw, Job.class);
            any = Iterables.any(jobs, new Predicate<Job>() {

                @Override
                public boolean apply(@Nullable Job job) {
                    String jobModelId = job.getInputs().get(WorkflowContextConstants.Inputs.MODEL_ID);
                    String jobModelName = job.getInputs().get(WorkflowContextConstants.Inputs.MODEL_DISPLAY_NAME);
                    return job.getJobType() != null && job.getJobType().equals(jobType) && modelId.equals(jobModelId)
                            && SelfServiceModelingEndToEndDeploymentTestNG.MODEL_DISPLAY_NAME.equals(jobModelName);
                }
            });

            if (any) {
                break;
            }
            sleep(500);
        }

        assertTrue(any);
    }

    private void sleep(long msec) {
        try {
            Thread.sleep(msec);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public class ScoreResult {

        private static final double EPS = 1e-6;

        public Double rawScore;
        public Double score;

        public ScoreResult(Double rawScore, Double score) {
            this.rawScore = rawScore;
            this.score = score;
        }

        public void setRawScore(Double rawScore) {
            this.rawScore = rawScore;
        }

        public void setScore(Double score) {
            this.score = score;
        }

        public Double getRawScore() {
            return this.rawScore;
        }

        public Double getScore() {
            return this.score;
        }

        @Override
        public String toString() {
            return JsonUtils.serialize(this);
        }

        @Override
        public boolean equals(Object object) {
            if (object == this) {
                return true;
            }
            if (!(object instanceof ScoreResult)) {
                return false;
            }
            ScoreResult score = (ScoreResult) object;
            return (Math.abs(this.rawScore - score.rawScore) <= EPS);
        }

    }

}
