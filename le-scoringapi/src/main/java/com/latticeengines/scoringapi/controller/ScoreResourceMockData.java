package com.latticeengines.scoringapi.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.scoringapi.FieldType;
import com.latticeengines.scoringapi.exposed.Field;
import com.latticeengines.scoringapi.exposed.Fields;
import com.latticeengines.scoringapi.exposed.Model;
import com.latticeengines.scoringapi.exposed.ModelType;
import com.latticeengines.scoringapi.exposed.ScoreResponse;

@Component
public class ScoreResourceMockData {
    public Map<ModelType, List<Model>> activeModelMap = new HashMap<>();
    public Map<String, Fields> modelFields = new HashMap<>();

    public static final String modelId1 = "ms__1bcd7c1d-1703-4704-9536-60728bdd9999-PLSModel";
    public static final String modelId2 = "ms__1bcd7c1d-1703-4704-9536-60728bdd9998-PLSModel";
    public static final String modelId3 = "ms__1bcd7c1d-1703-4704-9536-60728bdd9997-PLSModel";

    @PostConstruct
    public void initializeStubData() throws Exception {
        List<Model> contactModels = new ArrayList<>();
        contactModels.add(new Model(modelId1, "US Contact Model", ModelType.CONTACT));
        contactModels.add(new Model(modelId2, "EU Contact Model", ModelType.CONTACT));

        List<Model> accountModels = new ArrayList<>();
        accountModels.add(new Model("ms__1bcd7c1d-1703-4704-9536-60728bdd9997-PLSModel", "US Account Model",
                ModelType.ACCOUNT));

        activeModelMap.put(ModelType.ACCOUNT, accountModels);
        activeModelMap.put(ModelType.CONTACT, contactModels);

        Fields fields1 = new Fields();
        fields1.setModelId(modelId1);

        List<Field> fields = new ArrayList<>();
        fields.add(new Field("Email", FieldType.STRING));
        fields.add(new Field("Company", FieldType.STRING));
        fields.add(new Field("City", FieldType.STRING));
        fields.add(new Field("State", FieldType.STRING));
        fields.add(new Field("Country", FieldType.STRING));
        fields.add(new Field("CreatedDate", FieldType.LONG));
        fields.add(new Field("LastModifiedDate", FieldType.LONG));
        fields.add(new Field("PostalCode", FieldType.STRING));
        fields.add(new Field("FirstName", FieldType.STRING));
        fields.add(new Field("LastName", FieldType.STRING));
        fields.add(new Field("Title", FieldType.STRING));
        fields.add(new Field("LeadSource", FieldType.STRING));
        fields.add(new Field("Phone", FieldType.STRING));
        fields.add(new Field("AnnualRevenue", FieldType.FLOAT));
        fields.add(new Field("NumberOfEmployees", FieldType.INTEGER));
        fields.add(new Field("Industry", FieldType.STRING));

        fields1.setFields(fields);

        modelFields.put(modelId1, fields1);
        modelFields.put(modelId2, fields1);
        modelFields.put(modelId3, fields1);
    }

    public ScoreResponse simulateScore() {
        ScoreResponse scoreResponse = new ScoreResponse();
        scoreResponse.setScore(ThreadLocalRandom.current().nextInt(5, 99));
        return scoreResponse;
    }
}
