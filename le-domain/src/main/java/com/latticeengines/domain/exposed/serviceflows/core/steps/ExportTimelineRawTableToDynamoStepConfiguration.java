package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.latticeengines.domain.exposed.datafabric.GenericTableActivity;

public class ExportTimelineRawTableToDynamoStepConfiguration extends BaseExportToDynamoConfiguration {

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
    public boolean registerDataUnit() {
        return false;
    }
}
