package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.BOMInputStream;
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.SourceFile;
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

    private SourceFile sourceFile;

    private String fileName = "Mulesoft_MKTO_LP3_ModelingLead_OneLeadPerDomain_20160316_170113.csv";

    @BeforeClass(groups = "deployment.lp")
    public void setup() throws Exception {
        selfServiceModeling.setup();
        switchToSuperAdmin();
        Tenant tenant = selfServiceModeling.getTenant();
        System.out.println(tenant);
        String accessToken = getAccessToken(tenant.getId());
        // String accessToken = "01e4019e-81fb-40a7-b643-2288cf13c36d";
        System.out.println("access_token: " + accessToken);
        AuthorizationHeaderHttpRequestInterceptor interceptor = new AuthorizationHeaderHttpRequestInterceptor(
                accessToken);
        scoringRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { interceptor }));
    }

    @Test(groups = "deployment.lp", enabled = true)
    public void testEndToEnd() throws InterruptedException, IOException {
        selfServiceModeling.setFileName(fileName);
        System.out.println("Uploading File");
        selfServiceModeling.uploadFile();
        sourceFile = selfServiceModeling.getSourceFile();
        System.out.println(sourceFile.getName());
        System.out.println("Resolving Metadata");
        resolveMetadata();
        System.out.println("Creatinging Model");
        selfServiceModeling.createModel();
        selfServiceModeling.retrieveModelSummary();
        ModelSummary modelSummary = selfServiceModeling.getModelSummary();

        String modelId = modelSummary.getId();
        System.out.println("modeling id: " + modelId);

        String modelingAppId = modelSummary.getApplicationId();
        JobStatus modelingJobStatus = modelProxy.getJobStatus(modelingAppId);
        String modelResultDir = modelingJobStatus.getResultDirectory();
        // String modelResultDir =
        // "/user/s-analytics/customers/DevelopTestPLSTenant2.DevelopTestPLSTenant2.Production/models/RunMatchWithLEUniverse_152089_DerivedColumnsCache_with_std_attrib/9e3a9f01-73f4-43c5-b1e7-f09699ae4b06/1457979567285_0027";
        // String modelId = "ms__9e3a9f01-73f4-43c5-b1e7-f09699ae4b06-SelfServ";
        System.out.println("modeling result dir: " + modelResultDir);
        saveExpectedScores(modelResultDir);

        Fields fields = getModelingFields(modelId);
        System.out.println("fields: " + fields.getFields());
        List<Map<String, Object>> records = createRecords(fields.getFields());
        List<String> largeScoreVarianceList = new ArrayList<>();
        for (Map<String, Object> record : records) {
            System.out.println(record);
            ScoringResponse response = score(record, modelId);
            System.out.println("scoring response: " + response);
            compareScoreResult(record, response.getProbability(), largeScoreVarianceList);
        }
        for (String warning : largeScoreVarianceList) {
            System.out.println(warning);
        }
        System.out.println("Number of records compared:" + records.size());
        System.out.println("Number of records having scores largely different from modeling scores:"
                + largeScoreVarianceList.size());
        assertTrue(largeScoreVarianceList.size() < records.size(),
                "All Scores have high variance after comparing with modeling scores");

    }

    @SuppressWarnings("rawtypes")
    private void resolveMetadata() {
        RestTemplate restTemplate = selfServiceModeling.getRestTemplate();
        ResponseDocument response = restTemplate.getForObject(
                String.format("%s/pls/fileuploads/%s/metadata/unknown", getPLSRestAPIHostPort(), sourceFile.getName()),
                ResponseDocument.class);
        @SuppressWarnings("unchecked")
        List<LinkedHashMap> unknownColumns = new ObjectMapper().convertValue(response.getResult(), List.class);
        setUnknowColumnType(unknownColumns);
        response = restTemplate.postForObject(
                String.format("%s/pls/fileuploads/%s/metadata/unknown", getPLSRestAPIHostPort(), sourceFile.getName()),
                unknownColumns, ResponseDocument.class);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void setUnknowColumnType(List<LinkedHashMap> unknownColumns) {
        Set<String> booleanSet = Sets.newHashSet(new String[] { "Interest_esb__c", "Interest_tcat__c",
                "kickboxAcceptAll", "Free_Email_Address__c", "kickboxFree", "Unsubscribed", "kickboxDisposable",
                "HasAnypointLogin", "HasCEDownload", "HasEEDownload" });
        Set<String> strSet = Sets.newHashSet(new String[] { "Lead_Source_Asset__c", "kickboxStatus", "SICCode",
                "Source_Detail__c", "Cloud_Plan__c" });
        System.out.println(unknownColumns);
        for (LinkedHashMap<String, String> map : unknownColumns) {
            String columnName = map.get("columnName");
            if (booleanSet.contains(columnName)) {
                map.put("columnType", Schema.Type.BOOLEAN.name());
            } else if (strSet.contains(columnName)) {
                map.put("columnType", Schema.Type.STRING.name());
            } else if (columnName.startsWith("Activity_Count_")) {
                map.put("columnType", Schema.Type.INT.name());
            }
        }
        System.out.println(unknownColumns);
    }

    private void compareScoreResult(Map<String, Object> record, double probability, List<String> largeScoreVarianceList) {
        double expectedScore = expectedScores.get(record.get(InterfaceName.Id.name()));
        if (Math.abs(expectedScore - probability) > 0.0000001) {
            String warning = String.format("Record id %s has value %f, expected is %f",
                    record.get(InterfaceName.Id.name()), probability, expectedScore);
            largeScoreVarianceList.add(warning);
            System.out.println(warning);
        }
    }

    private List<Map<String, Object>> createRecords(List<Field> fields) throws IOException {
        List<Map<String, Object>> records = new ArrayList<>();
        Set<String> ids = expectedScores.keySet();
        System.out.println("ids: " + ids);
        CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',');
        ClassPathResource csvResource = new ClassPathResource(RESOURCE_BASE + "/" + fileName);
        try (CSVParser parser = new CSVParser(new InputStreamReader(new BOMInputStream(csvResource.getInputStream())),
                format)) {
            for (CSVRecord csvRecord : parser) {
                String id = csvRecord.get(InterfaceName.Id.name());
                if (ids.contains(id)) {
                    Map<String, Object> scoreRecord = new HashMap<>();
                    for (Field field : fields) {
                        String fieldName = field.getFieldName();
                        scoreRecord.put(fieldName, csvRecord.get(fieldName));
                        if (csvRecord.get(fieldName).equals("")
                                || fieldName.equalsIgnoreCase(InterfaceName.CreatedDate.name())
                                || fieldName.equalsIgnoreCase(InterfaceName.LastModifiedDate.name())) {
                            scoreRecord.put(fieldName, null);
                        }
                    }
                    records.add(scoreRecord);
                }
            }
        }
        return records;
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
        private List<Warning> warnings = new ArrayList<>();

        @JsonProperty("probability")
        private double probability;

        public double getProbability() {
            return probability;
        }

        @SuppressWarnings("unused")
        public void setProbability(double probability) {
            this.probability = probability;
        }

        @SuppressWarnings("unused")
        public double getScore() {
            return score;
        }

        @SuppressWarnings("unused")
        public void setScore(double score) {
            this.score = score;
        }

        @SuppressWarnings("unused")
        public List<Warning> getWarnings() {
            return warnings;
        }

        @SuppressWarnings("unused")
        public void setWarnings(List<Warning> warnings) {
            this.warnings = warnings;
        }

        public String toString() {
            return JsonUtils.serialize(this);
        }
    }

    private static class Warning {

        @JsonProperty("warning")
        private String warning;

        @JsonProperty("warning_description")
        private String description;

        @JsonIgnore
        private WarningCode code;

        @SuppressWarnings("unused")
        public Warning() {

        }

        @SuppressWarnings("unused")
        public Warning(WarningCode code, String[] params) {
            this.code = code;
            this.warning = code.getExternalCode();
            this.description = buildDescription(code, params);
        }

        @SuppressWarnings("unused")
        public String getWarning() {
            return warning;
        }

        @SuppressWarnings("unused")
        public void setWarning(String warning) {
            this.warning = warning;
        }

        @SuppressWarnings("unused")
        public String getDescription() {
            return description;
        }

        @SuppressWarnings("unused")
        public void setDescription(String description) {
            this.description = description;
        }

        public static String buildDescription(WarningCode code, String[] params) {
            String description = code.getDescription();

            for (int i = 0; i < params.length; i++) {
                description = description.replaceAll("\\{" + i + "\\}", params[i]);
            }
            return description;
        }
    }

    private enum WarningCode {

        // @formatter:off
        MISSING_COLUMN("missing_column",
                "Input record has fewer columns than what the model expects; missing columns: {0}"), MISSING_VALUE(
                "missing_value",
                "Input record has missing values for certain expected columns; columns with missing values: {0}"), NO_MATCH(
                "no_match",
                "No match found for record in Lattice Data Cloud; domain key values: {0}; matched key values: {1}"), MISMATCHED_DATATYPE(
                "mismatched_datatype", "Input record contains columns that do not match expected datatypes: {0}"), PUBLIC_DOMAIN(
                "public_domain", "A public domain key was passed in for this record: {0}"), EXTRA_FIELDS(
                "extra_fields", "Input record contains extra columns: {0}");
        // @formatter:on

        private String externalCode;

        private String description;

        WarningCode(String externalCode, String description) {
            this.externalCode = externalCode;
            this.description = description;
        }

        public String getExternalCode() {
            return externalCode;
        }

        public String getDescription() {
            return description;
        }
    }

}
