package com.latticeengines.domain.exposed.eai;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.metadata.Table;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({ @Type(value = HdfsToS3Configuration.class, name = "HdfsToS3Configuration"),
        @Type(value = HdfsToRedshiftConfiguration.class, name = "HdfsToRedshiftConfiguration"),
        @Type(value = HdfsToDynamoConfiguration.class, name = "HdfsToDynamoConfiguration"), })
public class ExportConfiguration extends EaiJobConfiguration {

    private ExportFormat exportFormat;
    @NotNull
    private ExportDestination exportDestination;
    private Table table;
    private String exportInputPath;
    @NotNull
    @NotEmptyString
    private String exportTargetPath;
    private boolean exportUsingDisplayName = Boolean.TRUE;
    private String exclusionColumns;

    @JsonProperty("export_format")
    public ExportFormat getExportFormat() {
        return exportFormat;
    }

    @JsonProperty("export_format")
    public void setExportFormat(ExportFormat exportFormat) {
        this.exportFormat = exportFormat;
    }

    @JsonProperty("export_destination")
    public ExportDestination getExportDestination() {
        return exportDestination;
    }

    @JsonProperty("export_destination")
    public void setExportDestination(ExportDestination exportDestination) {
        this.exportDestination = exportDestination;
    }

    @JsonProperty("export_input_path")
    public String getExportInputPath() {
        return this.exportInputPath;
    }

    @JsonProperty("export_input_path")
    public void setExportInputPath(String exportInputPath) {
        this.exportInputPath = exportInputPath;
    }

    @JsonProperty("export_target_path")
    public String getExportTargetPath() {
        return this.exportTargetPath;
    }

    @JsonProperty("export_target_path")
    public void setExportTargetPath(String exportTargetPath) {
        this.exportTargetPath = exportTargetPath;
    }

    @JsonProperty("table")
    public Table getTable() {
        return table;
    }

    @JsonProperty("table")
    public void setTable(Table table) {
        this.table = table;
    }

    @JsonProperty("use_display_name")
    public boolean getUsingDisplayName() {
        return exportUsingDisplayName;
    }

    @JsonProperty("use_display_name")
    public void setUsingDisplayName(boolean exportUsingDisplayName) {
        this.exportUsingDisplayName = exportUsingDisplayName;
    }

    @JsonProperty("exclusion_columns")
    public String getExclusionColumns() {
        return exclusionColumns;
    }

    @JsonProperty("exclusion_columns")
    public void setExclusionColumns(String exclusionColumns) {
        this.exclusionColumns = exclusionColumns;
    }

}
