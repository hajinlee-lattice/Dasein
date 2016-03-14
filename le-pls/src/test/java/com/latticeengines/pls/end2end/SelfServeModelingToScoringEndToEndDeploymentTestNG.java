package com.latticeengines.pls.end2end;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.support.HttpRequestWrapper;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.metadata.SemanticType;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.scoringapi.FieldType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.dataplatform.ModelProxy;

@Component
public class SelfServeModelingToScoringEndToEndDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Autowired
    private SelfServiceModelingEndToEndDeploymentTestNG selfServiceModeling;

    @Autowired
    private ModelProxy modelProxy;

    @Autowired
    private Configuration yarnConfiguration;

    private static final String RESOURCE_BASE = "com/latticeengines/pls/end2end/selfServiceModeling/csvfiles";

    private RestTemplate scoringRestTemplate = new RestTemplate();

    private Map<String, Double> expectedScores = new HashMap<>();

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceBaseDir;

    @Value("${pls.scoringapi.rest.endpoint.hostport}")
    private String scoringApiHostPort;

    @BeforeClass(groups = "deployment.lp")
    public void setup() throws Exception {
        selfServiceModeling.setup();
        switchToSuperAdmin();
        Tenant tenant = selfServiceModeling.getTenant();
        System.out.println(tenant);
        String accessToken = getAccessToken(tenant.getId());
        // String accessToken = "64935b4d-2e99-4b27-811e-27af6a9eb8b4";
        System.out.println("access_token: " + accessToken);
        AuthorizationHeaderHttpRequestInterceptor interceptor = new AuthorizationHeaderHttpRequestInterceptor(
                accessToken);
        scoringRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { interceptor }));
    }

    @Test(groups = "deployment.lp", enabled = false)
    public void testEndToEnd() throws InterruptedException, IOException {
        selfServiceModeling.uploadFile();
        selfServiceModeling.resolveMetadata();
        selfServiceModeling.createModel();

        ModelSummary modelSummary = selfServiceModeling.retrieveModelSummary();
        String modelId = modelSummary.getId();
        System.out.println("modeling id: " + modelId);

        String modelingAppId = modelSummary.getApplicationId();
        JobStatus modelingJobStatus = modelProxy.getJobStatus(modelingAppId);
        String modelResultDir = modelingJobStatus.getResultDirectory();
        // String modelResultDir =
        // "/user/s-analytics/customers/DevelopTestPLSTenant2.DevelopTestPLSTenant2.Production/models/RunMatchWithLEUniverse_151997_DerivedColumnsCache_with_std_attrib/b857be12-eeac-4b5e-b170-67fe1d3b3a9f/1457631683969_0092";
        // String modelId = "ms__b857be12-eeac-4b5e-b170-67fe1d3b3a9f-SelfServ";
        System.out.println("modeling result dir: " + modelResultDir);
        saveExpectedScores(modelResultDir);

        Fields fields = getModelingFields(modelId);
        System.out.println("fields: " + fields.getFields());
        List<Map<String, Object>> records = createRecords(fields.getFields());
        for (Map<String, Object> record : records) {
            System.out.println(record);
            ScoringResponse response = score(record, modelId);
            System.out.println("scoring response: " + response);
            compareScoreResult(record, response.getScore());
        }
    }

    private void compareScoreResult(Map<String, Object> record, double probability) {
        double expectedScore = expectedScores.get(record.get("Id"));
        if (Math.abs(expectedScore - probability) > 0.0000001) {
            System.out.println(String.format("Record id %d has value %f, expected is %f", record.get("Id"),
                    probability, expectedScore));
        }
    }

    private List<Map<String, Object>> createRecords(List<Field> fields) throws IOException {
        List<Map<String, Object>> records = new ArrayList<>();
        Set<String> ids = expectedScores.keySet();
        System.out.println("ids: " + ids);
        CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',');
        ClassPathResource csvResource = new ClassPathResource(RESOURCE_BASE + "/Mulesoft_SFDC_LP3_1000.csv");
        try (CSVParser parser = new CSVParser(new InputStreamReader(new BOMInputStream(csvResource.getInputStream())),
                format)) {
            for (CSVRecord csvRecord : parser) {
                String id = csvRecord.get(SemanticType.Id.name());
                if (ids.contains(id)) {
                    Map<String, Object> scoreRecord = new HashMap<>();
                    for (Field field : fields) {
                        String fieldName = field.getFieldName();
                        String csvHeader = getCSVHeader(fieldName);
                        if (!fieldName.equals(SemanticType.Domain.name())) {
                            scoreRecord.put(fieldName, csvRecord.get(csvHeader));
                        } else {
                            String email = csvRecord.get(SemanticType.Email.name());
                            String domain = StringUtils.substringAfter(email, "@");
                            scoreRecord.put(fieldName, domain);
                        }
                        if (csvRecord.get(csvHeader).equals("")) {
                            scoreRecord.put(fieldName, null);
                        }
                    }
                    records.add(scoreRecord);
                }
            }
        }
        return records;
    }

    private String getCSVHeader(String fieldName) {
        if (fieldName.equals(SemanticType.CompanyName.name())) {
            return "Company";
        } else if (fieldName.equals(SemanticType.Event.name())) {
            return "IsConverted";
        } else if (fieldName.equals(SemanticType.PhoneNumber.name())) {
            return "Phone";
        } else if (fieldName.endsWith(SemanticType.IsClosed.name())) {
            return "Closed";
        }
        return fieldName;
    }

    private String getAccessToken(String tenantId) {
        String accessToken = restTemplate.getForObject(getRestAPIHostPort() + "/pls/oauth2/accesstoken?tenantId="
                + tenantId, String.class);
        return accessToken;
    }

    private Fields getModelingFields(String modelId) {
        Fields fields = scoringRestTemplate.getForObject(scoringApiHostPort + "/score/models/" + modelId + "/fields",
                Fields.class);
        return fields;
    }

    private ScoringResponse score(Map<String, Object> record, String modelId) {
        ScoringRequest request = new ScoringRequest();
        request.setModelId(modelId);
        request.setRecord(record);
        ScoringResponse response = scoringRestTemplate.postForObject(scoringApiHostPort + "/score/record/debug",
                request, ScoringResponse.class);
        return response;
    }

    private void saveExpectedScores(String modelResultDir) throws IOException {
        List<String> scoreTextFileHdfsPath = HdfsUtils
                .getFilesForDir(yarnConfiguration, modelResultDir, ".*scored.txt");
        if (scoreTextFileHdfsPath.size() != 1) {
            throw new FileNotFoundException("scored.txt is not found.");
        }

        String resultString = HdfsUtils.getHdfsFileContents(yarnConfiguration, scoreTextFileHdfsPath.get(0));
        String[] rows = resultString.split("\n");
        for (String row : rows) {
            String[] columns = row.split(",");
            assert (columns.length == 2);
            String id = columns[0];
            Double score = Double.valueOf(columns[1]);
            expectedScores.put(id, score);
        }
    }

    public static class AuthorizationHeaderHttpRequestInterceptor implements ClientHttpRequestInterceptor {

        private String headerValue;

        public AuthorizationHeaderHttpRequestInterceptor(String headerValue) {
            this.headerValue = headerValue;
        }

        @Override
        public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution)
                throws IOException {
            HttpRequestWrapper requestWrapper = new HttpRequestWrapper(request);
            requestWrapper.getHeaders().add("Authorization", "Bearer " + headerValue);
            return execution.execute(requestWrapper, body);
        }
    }

    private static class Field {
        @JsonProperty("fieldName")
        private String fieldName;

        @JsonProperty("fieldType")
        private FieldType fieldType;

        @JsonProperty("fieldValue")
        private Object fieldValue;

        @SuppressWarnings("unused")
        public Field() {
        }

        @SuppressWarnings("unused")
        public Field(String fieldName, FieldType fieldType) {
            this.fieldName = fieldName;
            this.fieldType = fieldType;
        }

        public String getFieldName() {
            return fieldName;
        }

        @SuppressWarnings("unused")
        public FieldType getFieldType() {
            return fieldType;
        }

        public String toString() {
            return JsonUtils.serialize(this);
        }
    }

    private static class Fields {
        @JsonProperty("modelId")
        private String modelId;

        @JsonProperty("fields")
        private List<Field> fields;

        @SuppressWarnings("unused")
        public String getModelId() {
            return modelId;
        }

        @SuppressWarnings("unused")
        public void setModelId(String modelId) {
            this.modelId = modelId;
        }

        public List<Field> getFields() {
            return fields;
        }

        @SuppressWarnings("unused")
        public void setFields(List<Field> fields) {
            this.fields = fields;
        }
    }

    private static class ScoringRequest {
        @JsonProperty("modelId")
        private String modelId;

        @JsonProperty("record")
        private Map<String, Object> record;

        @SuppressWarnings("unused")
        public String getModelId() {
            return modelId;
        }

        public void setModelId(String modelId) {
            this.modelId = modelId;
        }

        @SuppressWarnings("unused")
        public Map<String, Object> getRecord() {
            return record;
        }

        public void setRecord(Map<String, Object> record) {
            this.record = record;
        }
    }

    private static class ScoringResponse {
        @JsonProperty("score")
        private double score;

        @JsonProperty("warnings")
        private List<String> warnings = new ArrayList<>();

        public double getScore() {
            return score;
        }

        @SuppressWarnings("unused")
        public void setScore(double score) {
            this.score = score;
        }

        @SuppressWarnings("unused")
        public List<String> getWarnings() {
            return warnings;
        }

        @SuppressWarnings("unused")
        public void setWarnings(List<String> warnings) {
            this.warnings = warnings;
        }
    }

}
