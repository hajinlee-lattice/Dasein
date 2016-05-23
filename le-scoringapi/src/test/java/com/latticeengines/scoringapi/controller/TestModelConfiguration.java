package com.latticeengines.scoringapi.controller;

import com.latticeengines.scoringapi.exposed.model.impl.ModelRetrieverImpl;

public class TestModelConfiguration {
    // private String testModelFolderName = "3MulesoftAllRows20160314_112802";
    // private String modelId = "ms__" + testModelFolderName + "_";
    // private String modelName = testModelFolderName;
    // private String localModelPath = "com/latticeengines/scoringapi/model/" +
    // testModelFolderName + "/";
    // private String applicationId = "application_1457046993615_3823";
    // private String parsedApplicationId = "1457046993615_3823";
    // private String modelVersion = "1ba99b36-c222-4f93-ab8a-6dcc11ce45e9";
    // private String eventTable = testModelFolderName;
    // private String sourceInterpretation = "SalesforceLead";
    // private String modelSummaryJsonLocalpath = localModelPath +
    // ModelRetrieverImpl.MODEL_JSON;

    private String testModelFolderName;
    private String modelId;
    private String modelName;
    private String localModelPath = "com/latticeengines/scoringapi/model/3MulesoftAllRows20160314_112802/";
    private String applicationId;
    private String parsedApplicationId;
    private String modelVersion;
    private String eventTable;
    private String sourceInterpretation;
    private String modelSummaryJsonLocalpath;

    public TestModelConfiguration(String testModelFolderName, String applicationId, String modelVersion) {
        this.testModelFolderName = testModelFolderName;
        this.modelId = "ms__" + testModelFolderName + "_";
        this.modelName = testModelFolderName;
        // this.localModelPath = "com/latticeengines/scoringapi/model/" +
        // testModelFolderName + "/";
        this.applicationId = applicationId;
        this.parsedApplicationId = applicationId.substring(applicationId.indexOf("_") + 1);
        this.modelVersion = modelVersion;
        this.eventTable = testModelFolderName;
        this.sourceInterpretation = "SalesforceLead";
        this.modelSummaryJsonLocalpath = localModelPath + ModelRetrieverImpl.MODEL_JSON;
    }

    public String getTestModelFolderName() {
        return testModelFolderName;
    }

    public String getModelId() {
        return modelId;
    }

    public String getModelName() {
        return modelName;
    }

    public String getLocalModelPath() {
        return localModelPath;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public String getParsedApplicationId() {
        return parsedApplicationId;
    }

    public String getModelVersion() {
        return modelVersion;
    }

    public String getEventTable() {
        return eventTable;
    }

    public String getSourceInterpretation() {
        return sourceInterpretation;
    }

    public String getModelSummaryJsonLocalpath() {
        return modelSummaryJsonLocalpath;
    }
}
