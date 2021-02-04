package com.latticeengines.domain.exposed.serviceflows.cdl.steps.publish;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ElasticSearchExportConfig;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;

public class ImportPublishTableFromS3StepConfiguration extends ImportExportS3StepConfiguration {

    @JsonProperty("ExportConfigs")
    private List<ElasticSearchExportConfig> exportConfigs;

    public List<ElasticSearchExportConfig> getExportConfigs() {
        return exportConfigs;
    }

    public void setExportConfigs(List<ElasticSearchExportConfig> exportConfigs) {
        this.exportConfigs = exportConfigs;
    }
}
