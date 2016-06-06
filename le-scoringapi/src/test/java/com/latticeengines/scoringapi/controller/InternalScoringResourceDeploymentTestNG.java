package com.latticeengines.scoringapi.controller;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.io.Files;
import com.latticeengines.common.exposed.util.JsonUtils;
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
import com.latticeengines.network.exposed.scoringapi.InternalScoringApiInterface;

public class InternalScoringResourceDeploymentTestNG extends ScoringResourceDeploymentTestNGBase {

    private static final String TEST_MODEL_NAME_PREFIX = "TestInternal3MulesoftAllRows";

    @Autowired
    private InternalScoringApiInterface internalScoringApiProxy;

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
        ScoreResponse scoreResponse = internalScoringApiProxy.scorePercentileRecord(scoreRequest,
                customerSpace.toString());
        Assert.assertEquals(scoreResponse.getScore(), EXPECTED_SCORE_99);
    }

    @Test(groups = "deployment", enabled = true)
    public void scoreDebugRecord() throws IOException {
        ScoreRequest scoreRequest = getScoreRequest();
        scoreRequest.setModelId(MODEL_ID);
        DebugScoreResponse scoreResponse = (DebugScoreResponse) internalScoringApiProxy
                .scoreProbabilityRecord(scoreRequest, customerSpace.toString());
        Assert.assertEquals(scoreResponse.getScore(), EXPECTED_SCORE_99);
        double difference = Math.abs(scoreResponse.getProbability() - 0.5411256857185404d);
        Assert.assertTrue(difference < 0.1);
    }

    @Test(groups = "deployment", enabled = true)
    public void scoreOutOfRangeRecord() throws IOException {
        URL scoreRequestUrl = ClassLoader.getSystemResource(LOCAL_MODEL_PATH + "outofrange_score_request.json");
        String scoreRecordContents = Files.toString(new File(scoreRequestUrl.getFile()), Charset.defaultCharset());
        ScoreRequest scoreRequest = JsonUtils.deserialize(scoreRecordContents, ScoreRequest.class);

        scoreRequest.setModelId(MODEL_ID);

        DebugScoreResponse scoreResponse = (DebugScoreResponse) internalScoringApiProxy
                .scoreProbabilityRecord(scoreRequest, customerSpace.toString());
        System.out.println(JsonUtils.serialize(scoreResponse));
        Assert.assertEquals(scoreResponse.getScore(), EXPECTED_SCORE_99);
        Assert.assertTrue(scoreResponse.getProbability() > 0.27);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "scoreRecords" })
    public void getModelFieldsAfterScoring() {
        List<Model> models = internalScoringApiProxy.getActiveModels(ModelType.CONTACT, customerSpace.toString());
        for (Model model : models) {
            Fields fields = internalScoringApiProxy.getModelFields(model.getModelId(), customerSpace.toString());
            checkFields(model.getName(), fields, TEST_MODEL_NAME_PREFIX, TestRegisterModels.DISPLAY_NAME_PREFIX);
        }
    }

    // @Test(groups = "deployment", enabled = true, dependsOnMethods = {
    // "scoreRecords" })
    public void getPaginatedModels() {
        List<ModelDetail> models = internalScoringApiProxy.getPaginatedModels(new Date(0), true, 0, 50,
                customerSpace.toString());
        checkModelDetails(models, TEST_MODEL_NAME_PREFIX, TestRegisterModels.DISPLAY_NAME_PREFIX);
    }

    @Test(groups = "deployment", enabled = true)
    public void getModelsCountAll() {
        baseAllModelCount = getModelCount(1, true, null, false);
    }

    @Test(groups = "deployment", enabled = true)
    public void getModelsCountActive() {
        baseAllActiveModelCount = getModelCount(1, false, null, false);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "scoreRecords" })
    public void getModelsCountAfterBulkScoring() {
        getModelCount(baseAllModelCount + MAX_MODELS, true, null, true);
        getModelCount(0, false, new Date(), true);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "scoreRecords", "getModelsCountAfterBulkScoring" })
    public void getModelsCountAfterModelDelete() {
        TestRegisterModels modelCreator = new TestRegisterModels();
        modelCreator.deleteModel(plsRest, customerSpace, MODEL_ID);
        getModelCount(baseAllActiveModelCount + MAX_MODELS - 1, false, null, true);
        getModelCount(0, false, new Date(), true);
    }

    private int getModelCount(int n, boolean considerAllStatus, Date lastUpdateTime, boolean shouldAssert) {
        int modelsCount = internalScoringApiProxy.getModelCount(lastUpdateTime, considerAllStatus,
                customerSpace.toString());
        if (shouldAssert) {
            Assert.assertTrue(modelsCount >= n);
        }
        return n;
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "getModels", "getModelsCountAll",
            "getModelsCountActive" })
    public void scoreRecords() throws IOException, InterruptedException {
        final String url = apiHostPort + "/scoreinternal/records";
        runScoringTest(url);
    }
}
