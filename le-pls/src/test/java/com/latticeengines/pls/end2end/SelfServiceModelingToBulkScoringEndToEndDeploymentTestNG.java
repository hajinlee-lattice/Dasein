package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.GzipUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class SelfServiceModelingToBulkScoringEndToEndDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final String RESOURCE_BASE = "com/latticeengines/pls/end2end/selfServiceModeling/csvfiles";
    private static final Log log = LogFactory.getLog(SelfServiceModelingEndToEndDeploymentTestNG.class);

    private static final int TOTAL_TRAINING_LINES = 1126;

    private static final int TOTAL_TESTING_LINES = 400;

    private static final String TRAINING_CSV_FILE = "Lattice_Relaunch_Small.csv";

    private static final String TESTING_CSV_FILE = "Lattice_Relaunch_Small_Testing.csv";

    @Autowired
    private SelfServiceModelingEndToEndDeploymentTestNG selfServiceModeling;

    @Autowired
    private Configuration yarnConfiguration;

    private String modelId;
    private String applicationId;
    private Long jobId;

    private String trainingFileName;
    private String testingFileName;

    private SourceFile sourceFile;

    private String jobType;

    @BeforeClass(groups = "deployment.lp")
    public void setup() throws Exception {
        selfServiceModeling.setup();
        trainingFileName = TRAINING_CSV_FILE;
        testingFileName = TESTING_CSV_FILE;
        modelId = selfServiceModeling.prepareModel(SchemaInterpretation.SalesforceLead, trainingFileName);
    }

    @Test(groups = "deployment.lp", enabled = true, timeOut = 600000)
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

    @Test(groups = "deployment.lp", dependsOnMethods = "testScoreTrainingDataUsingMR", enabled = true, timeOut = 600000)
    public void testScoreTestingDataUsingMR() throws Exception {
        System.out.println(String.format("%s/pls/scores/%s?fileName=%s", getRestAPIHostPort(), modelId,
                sourceFile.getName()));
        applicationId = selfServiceModeling.getRestTemplate().postForObject(
                String.format("%s/pls/scores/%s?fileName=%s", getRestAPIHostPort(), modelId, sourceFile.getName()), //
                null, String.class);
        applicationId = StringUtils.substringBetween(applicationId.split(":")[1], "\"");
        System.out.println(String.format("Score testing data applicationId = %s", applicationId));
        assertNotNull(applicationId);
        jobType = "importMatchAndScoreWorkflow";
        testScoreTestingDatea();
    }

    @Test(groups = "deployment.lp", dependsOnMethods = "testScoreTestingDataUsingMR", enabled = true, timeOut = 1200000)
    public void testScoreTrainingDataUsingRTS() throws Exception {
        System.out.println(String.format("%s/pls/scores/%s/training?useRtsApi=TRUE&performEnrichment=TRUE",
                getRestAPIHostPort(), modelId));
        applicationId = selfServiceModeling.getRestTemplate().postForObject(
                String.format("%s/pls/scores/%s/training?useRtsApi=TRUE&performEnrichment=TRUE", getRestAPIHostPort(),
                        modelId), //
                null, String.class);
        applicationId = StringUtils.substringBetween(applicationId.split(":")[1], "\"");
        System.out.println(String.format("Score training data applicationId = %s", applicationId));
        assertNotNull(applicationId);
        jobType = "rtsBulkScoreWorkflow";
        testScoreRTSTrainingData();
    }

    @Test(groups = "deployment.lp", dependsOnMethods = "testScoreTrainingDataUsingRTS", enabled = true, timeOut = 1200000)
    public void testScoreTestingDataUsingRTS() throws Exception {
        System.out.println(String.format("%s/pls/scores/%s?fileName=%s&useRtsApi=TRUE&performEnrichment=TRUE",
                getRestAPIHostPort(), modelId, sourceFile.getName()));
        applicationId = selfServiceModeling.getRestTemplate().postForObject(
                String.format("%s/pls/scores/%s?fileName=%s&useRtsApi=TRUE&performEnrichment=TRUE",
                        getRestAPIHostPort(), modelId, sourceFile.getName()), //
                null, String.class);
        applicationId = StringUtils.substringBetween(applicationId.split(":")[1], "\"");
        System.out.println(String.format("Score testing data applicationId = %s", applicationId));
        assertNotNull(applicationId);
        jobType = "importAndRTSBulkScoreWorkflow";
        testScoreTestingDatea();
    }

    private void testScoreMRTrainingData() throws IOException {
        testJobIsListed(jobType);
        poll();
        downloadCsv(TOTAL_TRAINING_LINES);

        uploadTestingDataFile();
    }

    private void testScoreRTSTrainingData() throws IOException {
        testJobIsListed(jobType);
        poll();
        downloadCsv(TOTAL_TRAINING_LINES);
        retrieveErrorsFile();
        uploadTestingDataFile();
    }

    private void testScoreTestingDatea() throws IOException {
        testJobIsListed(jobType);
        poll();
        downloadCsv(TOTAL_TESTING_LINES);
        retrieveErrorsFile();
    }

    private void retrieveErrorsFile() {
        // Relies on error in Account.csv
        restTemplate.getMessageConverters().add(new ByteArrayHttpMessageConverter());
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Arrays.asList(MediaType.ALL));
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<byte[]> response = restTemplate.exchange(
                String.format("%s/pls/scores/jobs/%s/errors", getRestAPIHostPort(), jobId), HttpMethod.GET, entity,
                byte[].class);
        assertEquals(response.getStatusCode(), HttpStatus.OK);
        String errors = new String(response.getBody());
        assertTrue(errors.length() > 0);
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
                        "SelfServiceScoring Test File.csv"), requestEntity, ResponseDocument.class);
        assertTrue(response.isSuccess());
        sourceFile = new ObjectMapper().convertValue(response.getResult(), SourceFile.class);
        log.info(sourceFile.getName());
    }

    private void downloadCsv(int totalRowNumber) throws IOException {
        selfServiceModeling.getRestTemplate().getMessageConverters().add(new ByteArrayHttpMessageConverter());
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Arrays.asList(MediaType.ALL));
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<byte[]> response = selfServiceModeling.getRestTemplate().exchange(
                String.format("%s/pls/scores/jobs/%d/results", getRestAPIHostPort(), jobId), HttpMethod.GET, entity,
                byte[].class);
        assertEquals(response.getStatusCode(), HttpStatus.OK);
        String results = new String(response.getBody());
        assertTrue(response.getHeaders().getFirst("Content-Disposition").contains("_scored.csv"));
        assertTrue(results.length() > 0);
        CSVParser parser = null;
        InputStream is = GzipUtils.decompressStream(new ByteArrayInputStream(response.getBody()));
        InputStreamReader reader = new InputStreamReader(is);
        CSVFormat format = LECSVFormat.format;
        try {
            parser = new CSVParser(reader, format);
            Set<String> csvHeaders = parser.getHeaderMap().keySet();
            assertTrue(csvHeaders.contains("Activity_Count_Click_Email"));
            assertTrue(csvHeaders.contains("Industry"));
            assertTrue(csvHeaders.contains("PhoneNumber"));
            assertTrue(csvHeaders.contains("Score"));
            assertFalse(csvHeaders.contains("RawScore"));

            int line = 1;
            for (CSVRecord record : parser.getRecords()) {
                assertTrue(StringUtils.isNotEmpty(record.get("Score")));
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

}
