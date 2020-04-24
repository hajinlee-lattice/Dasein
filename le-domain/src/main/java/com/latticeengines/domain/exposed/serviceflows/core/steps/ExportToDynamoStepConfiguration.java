package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.latticeengines.domain.exposed.datafabric.GenericTableEntity;

public class ExportToDynamoStepConfiguration extends BaseExportToDynamoConfiguration {

    @Override
    public Class<?> getEntityClass() {
        return GenericTableEntity.class;
    }

    @Override
    public String getRepoName() {
        return "GenericTable";
    }

    @Override
    public String getContextKey() {
        return "TABLES_GOING_TO_DYNAMO";
    }

    @Override
    public boolean needKeyPrefix() {
        return true;
    }
}
