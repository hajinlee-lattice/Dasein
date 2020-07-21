package com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata;

import java.util.List;

import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;

public class ImportDynamoTableFromS3Configuration extends ImportExportS3StepConfiguration {

    private List<String> tableNames;

    private String dynamoSignature;

    private String contextKey;

    public List<String> getTableNames() {
        return tableNames;
    }

    public void setTableNames(List<String> tableNames) {
        this.tableNames = tableNames;
    }

    public String getDynamoSignature() {
        return dynamoSignature;
    }

    public void setDynamoSignature(String dynamoSignature) {
        this.dynamoSignature = dynamoSignature;
    }

    public String getContextKey() {
        return contextKey;
    }

    public void setContextKey(String contextKey) {
        this.contextKey = contextKey;
    }
}
