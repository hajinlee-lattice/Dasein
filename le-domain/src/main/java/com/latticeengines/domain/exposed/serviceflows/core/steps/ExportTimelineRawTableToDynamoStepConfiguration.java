package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.latticeengines.domain.exposed.datafabric.GenericTableActivity;

public class ExportTimelineRawTableToDynamoStepConfiguration extends BaseExportToDynamoConfiguration {

    @Override
    public Class<?> getEntityClass() {
        return GenericTableActivity.class;
    }

    @Override
    public String getRepoName() {
        return "TimelineRawTable";
    }
}
