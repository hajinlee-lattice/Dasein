package com.latticeengines.scoringapi.debugtools;

import java.io.IOException;
import java.util.List;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.scoringapi.Field;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.FieldSource;
import com.latticeengines.domain.exposed.scoringapi.Fields;
import com.latticeengines.domain.exposed.scoringapi.Model;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;
import com.latticeengines.scoringapi.exposed.DebugScoreResponse;
import com.latticeengines.scoringapi.exposed.ScoreRequest;
import com.latticeengines.scoringapi.functionalframework.ScoringApiControllerDeploymentTestNGBase;

public class TestScoreDeploymentTestNG extends ScoringApiControllerDeploymentTestNGBase {

    @Test(groups = "deployment.lp", enabled = false)
    public void getModels() {
        String url = apiHostPort + "/score/models/CONTACT";
        ResponseEntity<List<Model>> response = oAuth2RestTemplate.exchange(url, HttpMethod.GET, null,
                new ParameterizedTypeReference<List<Model>>() {
                });
        List<Model> models = response.getBody();
        Assert.assertEquals(models.size(), 1);
        Assert.assertEquals(models.get(0).getModelId(), MODEL_ID);
    }

    @Test(groups = "deployment.lp", enabled = false)
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
            FieldSchema expectedSchema = eventTableDataComposition.fields.get(field.getFieldName());
            Assert.assertEquals(expectedSchema.type, field.getFieldType());
            Assert.assertEquals(expectedSchema.source, FieldSource.REQUEST);
        }
    }

    @Test(groups = "deployment.lp", enabled = false)
    public void scoreRecord() throws IOException {
        String url = apiHostPort + "/score/record";
        ScoreRequest scoreRequest = getScoreRequest();
        scoreRequest.setModelId(MODEL_ID);
        ResponseEntity<ScoreResponse> response = oAuth2RestTemplate.postForEntity(url, scoreRequest,
                ScoreResponse.class);

        @SuppressWarnings("unused")
        ScoreResponse scoreResponse = response.getBody();
    }

    @Test(groups = "deployment.lp", enabled = false)
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

}
