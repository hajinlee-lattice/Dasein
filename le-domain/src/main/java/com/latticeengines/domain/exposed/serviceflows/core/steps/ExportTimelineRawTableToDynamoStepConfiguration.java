package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.latticeengines.domain.exposed.datafabric.TimelineTableEntity;

public class ExportTimelineRawTableToDynamoStepConfiguration extends BaseExportToDynamoConfiguration {

    @Override
    public Class<?> getEntityClass() {
        return TimelineTableEntity.class;
    }

    @Override
    public String getRepoName() {
        return "TimelineRawTable";
    }
}
