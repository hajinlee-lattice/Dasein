package com.latticeengines.scoringapi.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpServerErrorException;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.scoringapi.exposed.Field;
import com.latticeengines.scoringapi.exposed.Fields;
import com.latticeengines.scoringapi.exposed.Model;
import com.latticeengines.scoringapi.exposed.ScoreRequest;
import com.latticeengines.scoringapi.exposed.ScoreResponse;
import com.latticeengines.scoringapi.functionalframework.ScoringApiControllerTestNGBase;

public class ScoringResourceTestNG extends ScoringApiControllerTestNGBase {

    private static final Log log = LogFactory.getLog(ScoringResourceTestNG.class);

    @Autowired
    private ScoreResourceMockData scoreResourceMockData;

    @Test(groups = "functional", enabled = false)
    public void getModels() {
        String url = apiHostPort + "/score/models/CONTACT";
        ResponseEntity<List<Model>> response = oAuth2RestTemplate.exchange(url, HttpMethod.GET, null,
                new ParameterizedTypeReference<List<Model>>() {
                });
        List<Model> models = response.getBody();
        Assert.assertNotNull(models);
        for (Model model : models) {
            log.info(model.getModelId());
        }
    }

    @Test(groups = "functional", enabled = false)
    public void getFields() {
        String modelId = "ms__7a5b24bb-8ff6-4797-98c4-4e1a045b1595-PLSModel";
        String url = apiHostPort + "/score/models/" + modelId + "/fields";
        ResponseEntity<Fields> response = oAuth2RestTemplate.exchange(url, HttpMethod.GET, null,
                new ParameterizedTypeReference<Fields>() {
                });
        Fields fields = response.getBody();
        Assert.assertNotNull(fields);
        log.info("modelId:" + fields.getModelId());
        for (Field field : fields.getFields()) {
            log.info(field.getFieldName() + " " + field.getFieldType());
        }
    }

    @Test(groups = "functional")
    public void scoreRecord() {
        String url = apiHostPort + "/score/record";
        ScoreRequest request = new ScoreRequest();
        request.setModelId("ms__7a5b24bb-8ff6-4797-98c4-4e1a045b1595-PLSModel");
        Map<String, Object> record = new HashMap<>();
        record.put("Email", "bob@amazon.com");
        record.put("LeadID", "bob123");

        request.setRecord(record);

        try {
            ResponseEntity<ScoreResponse> response = oAuth2RestTemplate
                    .postForEntity(url, request, ScoreResponse.class);

            ScoreResponse scoreResponse = response.getBody();
            Assert.assertNotNull(scoreResponse);
        } catch (HttpServerErrorException e) {
            System.out.println(e.getMessage());
            System.out.println(e.getStatusText());
            System.out.println(e.getStatusCode());
        }
    }

}
