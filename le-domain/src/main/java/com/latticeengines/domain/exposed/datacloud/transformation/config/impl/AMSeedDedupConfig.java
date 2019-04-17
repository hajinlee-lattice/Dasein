package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AMSeedDedupConfig extends TransformerConfig {
    @JsonProperty("IsDunsOnlyCleanup")
    private boolean isDunsOnlyCleanup;

    @JsonProperty("IsDomainOnlyCleanup")
    private boolean isDomainOnlyCleanup;

    @JsonProperty("IsDedup")
    private boolean isDedup;

    public boolean getIsDunsOnlyCleanup() {
        return isDunsOnlyCleanup;
    }

    public void setIsDunsOnlyCleanup(boolean isDunsOnlyCleanup) {
        this.isDunsOnlyCleanup = isDunsOnlyCleanup;
    }

    public boolean getIsDomainOnlyCleanup() {
        return isDomainOnlyCleanup;
    }

    public void setIsDomainOnlyCleanup(boolean isDomainOnlyCleanup) {
        this.isDomainOnlyCleanup = isDomainOnlyCleanup;
    }

    public boolean getIsDedup() {
        return isDedup;
    }

    public void setIsDedup(boolean isDedup) {
        this.isDedup = isDedup;
    }
}
