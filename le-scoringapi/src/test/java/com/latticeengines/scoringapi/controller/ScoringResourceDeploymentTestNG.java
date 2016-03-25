package com.latticeengines.scoringapi.controller;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.io.Files;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.FieldSource;
import com.latticeengines.scoringapi.exposed.DebugScoreResponse;
import com.latticeengines.scoringapi.exposed.Field;
import com.latticeengines.scoringapi.exposed.Fields;
import com.latticeengines.scoringapi.exposed.Model;
import com.latticeengines.scoringapi.exposed.ScoreRequest;
import com.latticeengines.scoringapi.exposed.ScoreResponse;
import com.latticeengines.scoringapi.functionalframework.ScoringApiControllerDeploymentTestNGBase;

public class ScoringResourceDeploymentTestNG extends ScoringApiControllerDeploymentTestNGBase {

    @Test(groups = "deployment", enabled = true)
    public void getModels() {
        String url = apiHostPort + "/score/models/CONTACT";
        ResponseEntity<List<Model>> response = oAuth2RestTemplate.exchange(url, HttpMethod.GET, null,
                new ParameterizedTypeReference<List<Model>>() {
                });
        List<Model> models = response.getBody();
        Assert.assertEquals(models.size(), 1);
        Assert.assertEquals(models.get(0).getModelId(), MODEL_ID);
    }

    @Test(groups = "deployment", enabled = true)
    public void getFields() {
        String modelId = MODEL_ID;
        String url = apiHostPort + "/score/models/" + modelId + "/fields";
        ResponseEntity<Fields> response = oAuth2RestTemplate.exchange(url, HttpMethod.GET, null,
                new ParameterizedTypeReference<Fields>() {
                });
        Fields fields = response.getBody();
        Assert.assertNotNull(fields);
        Assert.assertEquals(fields.getModelId(), modelId);

        for (Field field : fields.getFields()) {
            FieldSchema expectedSchema = dataScienceDataComposition.fields.get(field.getFieldName());
            Assert.assertEquals(expectedSchema.type, field.getFieldType());
            Assert.assertEquals(expectedSchema.source, FieldSource.REQUEST);
        }
    }

    @Test(groups = "deployment", enabled = true)
    public void scoreRecord() throws IOException {
        String url = apiHostPort + "/score/record";
        ScoreRequest scoreRequest = getScoreRequest();
        scoreRequest.setModelId(MODEL_ID);
        ResponseEntity<ScoreResponse> response = oAuth2RestTemplate.postForEntity(url, scoreRequest,
                ScoreResponse.class);

        ScoreResponse scoreResponse = response.getBody();
        Assert.assertEquals(scoreResponse.getScore(), 99.0d);
    }

    @Test(groups = "deployment", enabled = true)
    public void scoreDebugRecord() throws IOException {
        String url = apiHostPort + "/score/record/debug";
        ScoreRequest scoreRequest = getScoreRequest();
        scoreRequest.setModelId(MODEL_ID);
        ResponseEntity<DebugScoreResponse> response = oAuth2RestTemplate.postForEntity(url, scoreRequest,
                DebugScoreResponse.class);

        DebugScoreResponse scoreResponse = response.getBody();
        Assert.assertEquals(scoreResponse.getScore(), 99.0d);
        double difference = Math.abs(scoreResponse.getProbability() - 0.5411256857185404d);
        Assert.assertTrue(difference < 0.1);
    }

    @Test(groups = "deployment", enabled = true)
    public void scoreOutOfRangeRecord() throws IOException {
        String url = apiHostPort + "/score/record/debug";
        URL scoreRequestUrl = ClassLoader.getSystemResource(LOCAL_MODEL_PATH + "outofrange_score_request.json");
        String scoreRecordContents = Files.toString(new File(scoreRequestUrl.getFile()), Charset.defaultCharset());
        ScoreRequest scoreRequest = JsonUtils.deserialize(scoreRecordContents, ScoreRequest.class);

        scoreRequest.setModelId(MODEL_ID);
        ResponseEntity<DebugScoreResponse> response = oAuth2RestTemplate.postForEntity(url, scoreRequest,
                DebugScoreResponse.class);

        DebugScoreResponse scoreResponse = response.getBody();
        Assert.assertEquals(scoreResponse.getScore(), 99.0d);
    }


}
