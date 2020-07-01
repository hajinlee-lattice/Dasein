package com.latticeengines.domain.exposed.serviceflows.core.steps;

public class AtlasAccountLookupExportStepConfiguration extends BaseExportToDynamoConfiguration {

    public static final String NAME = "atlasAccountLookupExportStepConfiguration";

    @Override
    public boolean needEmptyFailed() {
        return false;
    }

    @Override
    public Class<?> getEntityClass() {
        return null;
    }

    @Override
    public String getRepoName() {
        return null;
    }

    @Override
    public String getContextKey() {
        return "ATLAS_ACCOUNT_LOOKUP_TO_DYNAMO";
    }

    @Override
    public boolean needKeyPrefix() {
        return false;
    }

    @Override
    public boolean registerDataUnit() {
        return true;
    }
}
