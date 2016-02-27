package com.latticeengines.scoringapi.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.scoringapi.exposed.Field;
import com.latticeengines.scoringapi.exposed.Fields;
import com.latticeengines.scoringapi.exposed.Model;
import com.latticeengines.scoringapi.functionalframework.ScoringApiFunctionalTestNGBase;

public class ScoringResourceTestNG extends ScoringApiFunctionalTestNGBase {

    @Autowired
    private ScoreResourceMockData scoreResourceMockData;

    @Test(groups = "functional", enabled = false)
    public void getModels() {
        String url = apiHostPort + "/score/models/CONTACT";
        ResponseEntity<List<Model>> response = oAuth2RestTemplate.exchange(url, HttpMethod.GET, null, new ParameterizedTypeReference<List<Model>>() {});
        List<Model> models = response.getBody();
        Assert.assertNotNull(models);
        for (Model model : models) {
            System.out.println(model.getModelId());
        }
    }

    @Test(groups = "functional")
    public void getFields() {
        String modelId = "ms__7a5b24bb-8ff6-4797-98c4-4e1a045b1595-PLSModel";
        String url = apiHostPort + "/score/models/" + modelId + "/fields";
        ResponseEntity<Fields> response = oAuth2RestTemplate.exchange(url, HttpMethod.GET, null, new ParameterizedTypeReference<Fields>() {});
        Fields fields = response.getBody();
        Assert.assertNotNull(fields);
        System.out.println(fields.getModelId());
        for (Field field : fields.getFields()) {
            System.out.println(field.getFieldName() + " " + field.getFieldType());
        }

        response = oAuth2RestTemplate.exchange(url, HttpMethod.GET, null, new ParameterizedTypeReference<Fields>() {});
        fields = response.getBody();
        Assert.assertNotNull(fields);
        System.out.println(fields.getModelId());
        for (Field field : fields.getFields()) {
            System.out.println(field.getFieldName() + " " + field.getFieldType());
        }
//        String result = oAuth2RestTemplate.getForObject(url, String.class);
//        Assert.assertNotNull(result);

    }
}
