package com.latticeengines.serviceflows.workflow.export;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;

public class ExportStepConfiguration extends MicroserviceStepConfiguration {
    @NotNull
    private ExportFormat exportFormat;

    @NotNull
    private ExportDestination exportDestination;

    private String exportInputPath;

    private String exportTargetPath;

    private String tableName;

    private boolean shouldUseDisplayName = Boolean.TRUE;

    private Map<String, String> properties = new HashMap<>();

    public ExportFormat getExportFormat() {
        return exportFormat;
    }

    public void setExportFormat(ExportFormat exportFormat) {
        this.exportFormat = exportFormat;
    }

    public ExportDestination getExportDestination() {
        return exportDestination;
    }

    public void setExportDestination(ExportDestination exportDestination) {
        this.exportDestination = exportDestination;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public void putProperty(String property, String value) {
        properties.put(property, value);
    }

    public String getExportInputPath() {
        return exportInputPath;
    }

    public void setExportInputPath(String exportInputPath) {
        this.exportInputPath = exportInputPath;
    }

    public String getExportTargetPath() {
        return exportTargetPath;
    }

    public void setExportTargetPath(String exportTargetPath) {
        this.exportTargetPath = exportTargetPath;
    }

    public boolean getUsingDisplayName() {
        return shouldUseDisplayName;
    }

    public void setUsingDisplayName(boolean shouldUseDisplayName) {
        this.shouldUseDisplayName = shouldUseDisplayName;
    }
}
