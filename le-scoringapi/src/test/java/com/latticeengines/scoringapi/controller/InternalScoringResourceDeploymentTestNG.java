package com.latticeengines.scoringapi.controller;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.scoringapi.DebugScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.Field;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.FieldSource;
import com.latticeengines.domain.exposed.scoringapi.Fields;
import com.latticeengines.domain.exposed.scoringapi.Model;
import com.latticeengines.domain.exposed.scoringapi.ModelDetail;
import com.latticeengines.domain.exposed.scoringapi.ModelType;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;

@Component
public class InternalScoringResourceDeploymentTestNG extends ScoringResourceDeploymentTestNGBase {

    private static final String TEST_MODEL_NAME_PREFIX = "TestInternal3MulesoftAllRows";
    private static ObjectMapper om = new ObjectMapper();

    @Value("${scoringapi.ratelimit.bulk.requests.max:20}")
    private int ratelimit;

    @Test(groups = "deployment", enabled = true)
    public void getAllLeadEnrichmentAttributes() {
        List<LeadEnrichmentAttribute> enrichmentAttributeList = internalResourceRestApiProxy
                .getLeadEnrichmentAttributes(customerSpace, null, null, false, false);
        Assert.assertNotNull(enrichmentAttributeList);
        Assert.assertTrue(enrichmentAttributeList.size() > 0);

        for (LeadEnrichmentAttribute attr : enrichmentAttributeList) {
            if (selectedAttributes.contains(attr.getFieldName())) {
                Assert.assertTrue(attr.getIsSelected());
            } else {
                Assert.assertFalse(attr.getIsSelected());
            }
            Assert.assertNotNull(attr.getFieldName());
            Assert.assertNotNull(attr.getCategory());
        }
    }

    @Test(groups = "deployment", enabled = true)
    public void getSelectedLeadEnrichmentAttributes() {
        List<LeadEnrichmentAttribute> enrichmentAttributeList = internalResourceRestApiProxy
                .getLeadEnrichmentAttributes(customerSpace, null, null, true, false);
        Assert.assertNotNull(enrichmentAttributeList);
        Assert.assertEquals(enrichmentAttributeList.size(), selectedAttributes.size());
        for (LeadEnrichmentAttribute attr : enrichmentAttributeList) {
            Assert.assertTrue(selectedAttributes.contains(attr.getFieldName()));
        }
    }

    @Test(groups = "deployment", enabled = true)
    public void getModels() {
        List<Model> models = internalScoringApiProxy.getActiveModels(ModelType.CONTACT, customerSpace.toString());
        Assert.assertEquals(models.size(), 1);
        Assert.assertEquals(models.get(0).getModelId(), MODEL_ID);
        Assert.assertEquals(models.get(0).getName(), MODEL_NAME);
    }

    @Test(groups = "deployment", enabled = true)
    public void getFields() {
        String modelId = MODEL_ID;

        Fields fields = internalScoringApiProxy.getModelFields(modelId, customerSpace.toString());
        Assert.assertNotNull(fields);
        Assert.assertEquals(fields.getModelId(), modelId);
        Assert.assertNotNull(fields.getValidationExpression());
        for (Field field : fields.getFields()) {
            FieldSchema expectedSchema = eventTableDataComposition.fields.get(field.getFieldName());
            Assert.assertEquals(expectedSchema.type, field.getFieldType());
            Assert.assertEquals(expectedSchema.source, FieldSource.REQUEST);
        }
    }

    @Test(groups = "deployment", enabled = true)
    public void scoreRecord() throws IOException {
        ScoreRequest scoreRequest = getScoreRequest();
        scoreRequest.setModelId(MODEL_ID);
        ObjectMapper om = new ObjectMapper();
        System.out.println(om.writeValueAsString(scoreRequest));
        ScoreResponse scoreResponse = internalScoringApiProxy.scorePercentileRecord(scoreRequest,
                customerSpace.toString(), true, false);
        Assert.assertEquals(scoreResponse.getScore(), EXPECTED_SCORE_67);
        Assert.assertNotNull(scoreResponse.getEnrichmentAttributeValues());
        Assert.assertTrue(scoreResponse.getEnrichmentAttributeValues().size() == 0);
        Assert.assertNotNull(scoreResponse.getBucket());
        Assert.assertEquals(scoreResponse.getBucket(), BucketName.A.toValue());
    }

    @Test(groups = "deployment", enabled = true)
    public void scoreDebugRecord() throws IOException {
        ScoreRequest scoreRequest = getScoreRequest();
        scoreRequest.setModelId(MODEL_ID);
        DebugScoreResponse scoreResponse = (DebugScoreResponse) internalScoringApiProxy
                .scoreProbabilityRecord(scoreRequest, customerSpace.toString(), true, false);
        Assert.assertEquals(scoreResponse.getScore(), EXPECTED_SCORE_67);
        double difference = Math.abs(scoreResponse.getProbability() - 0.41640343016092707d);
        Assert.assertTrue(difference < 0.1);
        Assert.assertNotNull(scoreResponse.getEnrichmentAttributeValues());
        Assert.assertTrue(scoreResponse.getEnrichmentAttributeValues().size() == 0);
        Assert.assertNotNull(scoreResponse.getBucket());
        Assert.assertEquals(scoreResponse.getBucket(), BucketName.A.toValue());
    }

    @Test(groups = "deployment", enabled = true)
    public void scoreRecordWithEnrichment() throws IOException {
        ScoreRequest scoreRequest = getScoreRequest();
        scoreRequest.setModelId(MODEL_ID);
        scoreRequest.setPerformEnrichment(true);
        System.out.println(om.writeValueAsString(scoreRequest));
        ScoreResponse scoreResponse = internalScoringApiProxy.scorePercentileRecord(scoreRequest,
                customerSpace.toString(), true, false);
        Assert.assertEquals(scoreResponse.getScore(), EXPECTED_SCORE_67);
        Assert.assertNotNull(scoreResponse.getEnrichmentAttributeValues());
        System.out.println("scoreResponse.getEnrichmentAttributeValues().size() = "
                + scoreResponse.getEnrichmentAttributeValues().size() + "\n\n" + om.writeValueAsString(scoreResponse));
        Assert.assertTrue(scoreResponse.getEnrichmentAttributeValues().size() == 6);
        Assert.assertNotNull(scoreResponse.getBucket());
        Assert.assertEquals(scoreResponse.getBucket(), BucketName.A.toValue());
    }

    @Test(groups = "deployment", enabled = true)
    public void scoreDebugRecordWithEnrichment() throws IOException {
        ScoreRequest scoreRequest = getScoreRequest();
        scoreRequest.setModelId(MODEL_ID);
        scoreRequest.setPerformEnrichment(true);
        DebugScoreResponse scoreResponse = (DebugScoreResponse) internalScoringApiProxy
                .scoreProbabilityRecord(scoreRequest, customerSpace.toString(), true, false);
        Assert.assertEquals(scoreResponse.getScore(), EXPECTED_SCORE_67);
        double difference = Math.abs(scoreResponse.getProbability() - 0.41640343016092707d);
        Assert.assertTrue(difference < 0.1);
        Assert.assertNotNull(scoreResponse.getEnrichmentAttributeValues());
        System.out.println("scoreResponse.getEnrichmentAttributeValues().size() = "
                + scoreResponse.getEnrichmentAttributeValues().size() + "\n\n" + om.writeValueAsString(scoreResponse));
        Assert.assertTrue(scoreResponse.getEnrichmentAttributeValues().size() == 6);
        Assert.assertNotNull(scoreResponse.getBucket());
        Assert.assertEquals(scoreResponse.getBucket(), BucketName.A.toValue());
    }

    @Test(groups = "deployment", enabled = true)
    public void scoreOutOfRangeRecord() throws IOException {
        InputStream scoreRequestStream = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(LOCAL_MODEL_PATH + "outofrange_score_request.json");
        String scoreRecordContents = IOUtils.toString(scoreRequestStream, Charset.defaultCharset());
        ScoreRequest scoreRequest = JsonUtils.deserialize(scoreRecordContents, ScoreRequest.class);

        scoreRequest.setModelId(MODEL_ID);

        DebugScoreResponse scoreResponse = internalScoringApiProxy.scoreProbabilityRecord(scoreRequest,
                customerSpace.toString(), true, false);
        System.out.println(JsonUtils.serialize(scoreResponse));
        Assert.assertEquals(scoreResponse.getScore(), EXPECTED_SCORE_89);
        Assert.assertTrue(scoreResponse.getProbability() > 0.27);
        Assert.assertNotNull(scoreResponse.getBucket());
        Assert.assertEquals(scoreResponse.getBucket(), BucketName.A.toValue());
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "scoreRecords" })
    public void getModelFieldsAfterScoring() {
        List<Model> models = internalScoringApiProxy.getActiveModels(ModelType.CONTACT, customerSpace.toString());
        for (Model model : models) {
            Fields fields = internalScoringApiProxy.getModelFields(model.getModelId(), customerSpace.toString());
            checkFields(model.getName(), fields, TEST_MODEL_NAME_PREFIX, TestRegisterModels.DISPLAY_NAME_PREFIX);
        }
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "scoreRecords" })
    public void getPaginatedModels() throws ParseException {
        Long start = System.currentTimeMillis();
        List<ModelDetail> models = internalScoringApiProxy.getPaginatedModels(new Date(0), true, 1, 50,
                customerSpace.toString());
        System.out.println("Time taken in getPaginatedModels for " + models.size() + " models = "
                + (System.currentTimeMillis() - start) + " ms");
        checkModelDetails(models, TEST_MODEL_NAME_PREFIX, TestRegisterModels.DISPLAY_NAME_PREFIX);
    }

    @Test(groups = "deployment", enabled = true)
    public void getModelsCountAll() {
        baseAllModelCount = getModelCount(1, true, null, false);
        Assert.assertEquals(baseAllModelCount, 1);
    }

    @Test(groups = "deployment", enabled = true)
    public void getModelsCountActive() {
        baseAllActiveModelCount = getModelCount(1, false, null, false);
        Assert.assertEquals(baseAllActiveModelCount, 1);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "scoreRecords" })
    public void getModelsCountAfterBulkScoring() {
        getModelCount(baseAllModelCount + MAX_MODELS, true, null, true);
        getModelCount(0, false, new Date(), true);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "scoreRecords", "getModelsCountAfterBulkScoring",
            "getPaginatedModels" })
    public void getModelsCountAfterModelDelete() {
        TestRegisterModels modelCreator = new TestRegisterModels();
        modelCreator.deleteModel(modelSummaryProxy, customerSpace, MODEL_ID);
        getModelCount(baseAllActiveModelCount + MAX_MODELS - 1, false, null, true);
        getModelCount(0, false, new Date(), true);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "getModels", "getModelsCountAll",
            "getModelsCountActive" })
    public void scoreRecords() throws IOException, InterruptedException {
        final String url = apiHostPort + "/scoreinternal/records";
        runScoringTest(url, true, false);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "scoreRecords" })
    public void scoreRecordsWithInternalEnrichmentFlag() throws IOException, InterruptedException {
        final String url = apiHostPort + "/scoreinternal/records";
        testScore(url, 10, 0, modelList, true, false, customerSpace, false, true);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "scoreRecordsWithInternalEnrichmentFlag" })
    public void testScoreLoadLimitReached() throws IOException, InterruptedException {
        final String url = apiHostPort + "/scoreinternal/records";
        runScoreLoadLimitTest(url, true, ratelimit);
    }

    private int getModelCount(int n, boolean considerAllStatus, Date lastUpdateTime, boolean shouldAssert) {
        int modelsCount = internalScoringApiProxy.getModelCount(lastUpdateTime, considerAllStatus,
                customerSpace.toString());
        if (shouldAssert) {
            Assert.assertTrue(modelsCount >= n);
        }
        return n;
    }
}
