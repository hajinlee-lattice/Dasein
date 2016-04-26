package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
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
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.workflow.ScoreWorkflowSubmitter;
import com.latticeengines.workflow.exposed.WorkflowContextConstants;

public class SelfServiceModelingToBulkScoringEndToEndDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final String RESOURCE_BASE = "com/latticeengines/pls/end2end/selfServiceModeling/csvfiles";
    private static final Log log = LogFactory.getLog(SelfServiceModelingEndToEndDeploymentTestNG.class);

    // expected lines in data set to output to csv after bulk scoring
    private static final int TOTAL_QUALIFIED_LINES = 4161;

    @Autowired
    private SelfServiceModelingEndToEndDeploymentTestNG selfServiceModeling;

    private String modelId;
    private String applicationId;
    private Long jobId;

    private String fileName;

    private SourceFile sourceFile;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ScoreWorkflowSubmitter scoreWorkflowSubmitter;

    @BeforeClass(groups = "deployment.lp")
    public void setup() throws Exception {
        selfServiceModeling.setup();
        modelId = selfServiceModeling.prepareModel(SchemaInterpretation.SalesforceLead, null, null);
        fileName = "Hootsuite_PLS132_LP3_ScoringLead_20160330_165806_modified.csv";
    }

    @Test(groups = "deployment.lp")
    public void testScoreTrainingData() throws Exception {
        System.out.println(String.format("%s/pls/scores/%s/training", getRestAPIHostPort(), modelId));
        applicationId = selfServiceModeling.getRestTemplate().postForObject(
                String.format("%s/pls/scores/%s/training", getRestAPIHostPort(), modelId), //
                null, String.class);
        applicationId = StringUtils.substringBetween(applicationId.split(":")[1], "\"");
        System.out.println(String.format("Score training data applicationId = %s", applicationId));
        assertNotNull(applicationId);
    }

    @Test(groups = "deployment.lp", dependsOnMethods = "testScoreTrainingData", timeOut = 60000)
    public void testJobIsListed() {
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
                    return job.getJobType() != null && job.getJobType().equals("scoreWorkflow")
                            && modelId.equals(jobModelId);
                }
            });

            if (any) {
                break;
            }
            sleep(500);
        }

        assertTrue(any);
    }

    @Test(groups = "deployment.lp", dependsOnMethods = "testJobIsListed", timeOut = 1800000)
    public void poll() {
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

    @Test(groups = "deployment.lp", dependsOnMethods = "poll")
    public void downloadCsv() throws IOException {
        selfServiceModeling.getRestTemplate().getMessageConverters().add(new ByteArrayHttpMessageConverter());
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Arrays.asList(MediaType.ALL));
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<byte[]> response = selfServiceModeling.getRestTemplate().exchange(
                String.format("%s/pls/scores/jobs/%d/results", getRestAPIHostPort(), jobId), HttpMethod.GET, entity,
                byte[].class);
        assertEquals(response.getStatusCode(), HttpStatus.OK);
        String results = new String(response.getBody());
        assertTrue(results.length() > 0);
        CSVParser parser = null;
        InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(response.getBody()));
        CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',');
        try {
            parser = new CSVParser(reader, format);
            Set<String> csvHeaders = parser.getHeaderMap().keySet();
            assertTrue(csvHeaders.contains("LEAD"));
            assertTrue(csvHeaders.contains("Phone"));
            assertTrue(csvHeaders.contains("Some Column"));
            int line = 1;
            for (CSVRecord record : parser.getRecords()) {
                assertTrue(StringUtils.isNotEmpty(record.get("Percentile")));
                line++;
            }
            assertEquals(line, TOTAL_QUALIFIED_LINES);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            parser.close();
        }
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = "deployment.lp", dependsOnMethods = "downloadCsv")
    public void uploadTestingDataFile() {
        LinkedMultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
        map.add("file", new ClassPathResource(RESOURCE_BASE + "/" + fileName));
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

    @Test(groups = "deployment.lp", dependsOnMethods = "uploadTestingDataFile", enabled = true)
    public void testScoreTestingData() throws Exception {
        System.out.println(String.format("%s/pls/scores/%s", getRestAPIHostPort(), modelId));
        applicationId = selfServiceModeling.getRestTemplate().postForObject(
                String.format("%s/pls/scores/%s?fileName=%s", getRestAPIHostPort(), modelId, sourceFile.getName()), //
                null, String.class);
        applicationId = StringUtils.substringBetween(applicationId.split(":")[1], "\"");
        System.out.println(String.format("Score testing data applicationId = %s", applicationId));
        assertNotNull(applicationId);
    }

    @Test(groups = "deployment.lp", dependsOnMethods = "testScoreTestingData", timeOut = 60000, enabled = true)
    public void testScoringTestDataJobIsListed() {
        testJobIsListed();
    }

    @Test(groups = "deployment.lp", dependsOnMethods = "testScoringTestDataJobIsListed", timeOut = 1800000, enabled = true)
    public void pollScoringTestDataJob() {
        poll();
    }

    @Test(groups = "deployment.lp", dependsOnMethods = "pollScoringTestDataJob", enabled = true)
    public void downloadTestingDataScoreResultCsv() throws IOException {
        downloadCsv();
    }

    private void sleep(long msec) {
        try {
            Thread.sleep(msec);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
