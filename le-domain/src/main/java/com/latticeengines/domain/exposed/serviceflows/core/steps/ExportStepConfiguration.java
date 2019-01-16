package com.latticeengines.domain.exposed.serviceflows.core.steps;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.common.exposed.validator.annotation.NotNull;

public class ExportStepConfiguration extends MicroserviceStepConfiguration {
    @NotNull
    private ExportFormat exportFormat = ExportFormat.CSV;

    @NotNull
    private ExportDestination exportDestination = ExportDestination.FILE;

    private String exportInputPath;

    private String exportTargetPath;

    private String tableName;

    private boolean shouldUseDisplayName = Boolean.TRUE;

    private boolean exportMergedFile = Boolean.FALSE;

    private String mergedFileName;

    private Map<String, Attribute> nameToAttributeMap;

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

    public String getProperty(String key) {
        return properties.get(key);
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

    public boolean isExportMergedFile() {
        return exportMergedFile;
    }

    public void setExportMergedFile(boolean exportMergedFile) {
        this.exportMergedFile = exportMergedFile;
    }

    public String getMergedFileName() {
        return mergedFileName;
    }

    public void setMergedFileName(String mergedFileName) {
        this.mergedFileName = mergedFileName;
    }

    public Map<String, Attribute> getNameToAttributeMap() {
        return nameToAttributeMap;
    }

    public void setAttributes(Map<String, Attribute> nameToAttributeMap) {
        this.nameToAttributeMap = nameToAttributeMap;
    }
}
