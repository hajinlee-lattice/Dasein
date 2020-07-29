package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import com.latticeengines.domain.exposed.serviceflows.core.steps.BaseExportToDynamoConfiguration;

public class AccountLookupToDynamoStepConfiguration extends BaseExportToDynamoConfiguration {

    public static final String NAME = "AccountLookupToDynamoStepConfiguration";

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
        return null;
    }

    @Override
    public boolean needKeyPrefix() {
        return false;
    }

    @Override
    public boolean getRegisterDataUnit() {
        return false;
    }
}
