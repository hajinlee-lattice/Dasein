package com.latticeengines.propdata.workflow.engine.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.ingestion.ProviderConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class IngestionStepConfiguration extends BaseStepConfiguration {
    @NotNull
    private IngestionProgress ingestionProgress;

    @NotNull
    private Ingestion ingestion;

    @NotNull
    private ProviderConfiguration providerConfiguration;

    @JsonProperty("ingestion_progress")
    public IngestionProgress getIngestionProgress() {
        return ingestionProgress;
    }

    @JsonProperty("ingestion_progress")
    public void setIngestionProgress(IngestionProgress progress) {
        this.ingestionProgress = progress;
    }

    @JsonProperty("ingestion")
    public Ingestion getIngestion() {
        return ingestion;
    }

    @JsonProperty("ingestion")
    public void setIngestion(Ingestion ingestion) {
        this.ingestion = ingestion;
    }

    @JsonProperty("providerConfiguration")
    public ProviderConfiguration getProviderConfiguration() {
        return providerConfiguration;
    }

    @JsonProperty("providerConfiguration")
    public void setProviderConfiguration(ProviderConfiguration providerConfiguration) {
        this.providerConfiguration = providerConfiguration;
    }

}
