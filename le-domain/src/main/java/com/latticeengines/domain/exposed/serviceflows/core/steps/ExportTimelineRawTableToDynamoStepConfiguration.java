package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datafabric.GenericTableActivity;

public class ExportTimelineRawTableToDynamoStepConfiguration extends BaseExportToDynamoConfiguration {

    public static final String NAME = "ExportTimelineRawTableToDynamoStepConfiguration";

    @JsonProperty("registerDataUnit")
    private boolean registerDataUnit;

    @Override
    public boolean needEmptyFailed() {
        return false;
    }

    @Override
    public Class<?> getEntityClass() {
        return GenericTableActivity.class;
    }

    @Override
    public String getRepoName() {
        return "GenericTable";
    }

    @Override
    public String getContextKey() {
        return "TIMELINE_RAWTABLES_GOING_TO_DYNAMO";
    }

    @Override
    public boolean needKeyPrefix() {
        return false;
    }

    @Override
    public boolean getRegisterDataUnit() {
        return registerDataUnit;
    }

    public void setRegisterDataUnit(boolean registerDataUnit) {
        this.registerDataUnit = registerDataUnit;
    }
}
