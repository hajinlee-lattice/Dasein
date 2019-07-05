package com.latticeengines.scoringapi.controller;

import com.latticeengines.scoringapi.exposed.model.impl.ModelRetrieverImpl;

public class TestModelConfiguration {
    private String testModelFolderName;
    private String modelId;
    private String modelName;
    private String localModelPath = "com/latticeengines/scoringapi/model/3MulesoftAllRows20160314_112802/";
    private String applicationId;
    private String parsedApplicationId;
    private String modelVersion;
    private String eventTable;
    private String sourceInterpretation;
    private String modelJsonLocalpath;
    private String modelSummaryJsonLocalpath;

    public TestModelConfiguration(String testModelFolderName, String modelId, String applicationId,
            String modelVersion) {
        this.testModelFolderName = testModelFolderName;
        this.modelId = modelId;
        this.modelName = testModelFolderName;
        this.applicationId = applicationId;
        this.parsedApplicationId = applicationId.substring(applicationId.indexOf("_") + 1);
        this.modelVersion = modelVersion;
        this.eventTable = testModelFolderName;
        this.sourceInterpretation = "SalesforceLead";
        this.modelJsonLocalpath = localModelPath + ModelRetrieverImpl.MODEL_JSON;
        this.modelSummaryJsonLocalpath = localModelPath + ModelRetrieverImpl.MODEL_SUMMARY_JSON;
    }

    public TestModelConfiguration(String testModelFolderName, String applicationId, String modelVersion) {
        this(testModelFolderName, "ms__" + testModelFolderName + "_", applicationId, modelVersion);
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

    public String getModelJsonLocalpath() {
        return modelJsonLocalpath;
    }

    public String getModelSummaryJsonLocalpath() {
        return modelSummaryJsonLocalpath;
    }
    
}
