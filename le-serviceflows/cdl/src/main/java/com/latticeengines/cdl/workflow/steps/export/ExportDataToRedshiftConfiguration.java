package com.latticeengines.cdl.workflow.steps.export;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;

public class ExportDataToRedshiftConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("initial_load")
    private boolean initialLoad = false;

    @JsonProperty("cleanup_s3")
    private boolean cleanupS3 = false;

    @JsonProperty("source_tables")
    private List<Table> sourceTables = new ArrayList<>();

    public boolean isInitialLoad() {
        return initialLoad;
    }

    public void setInitialLoad(boolean initialLoad) {
        this.initialLoad = initialLoad;
    }

    public boolean isCleanupS3() {
        return cleanupS3;
    }

    public void setCleanupS3(boolean cleanupS3) {
        this.cleanupS3 = cleanupS3;
    }

    public List<Table> getSourceTables() {
        return sourceTables;
    }

    public void setSourceTables(List<Table> sourceTables) {
        this.sourceTables = sourceTables;
    }
}
