package com.latticeengines.domain.exposed.dataloader;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SourceTableMetadataResult {

    @JsonProperty("Success")
    private boolean success;

    @JsonProperty("ErrorMessage")
    private String errorMessage;

    @JsonProperty("Metadata")
    private List<SourceColumnMetadata> metadata;

    public boolean getSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getErrorMessage() {
        return this.errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public List<SourceColumnMetadata> getMetadata() {
        return this.metadata;
    }

    public void setMetadata(List<SourceColumnMetadata> metadata) {
        this.metadata = metadata;
    }

    public static class SourceColumnMetadata {

        @JsonProperty("ColumnName")
        private String columnName;

        @JsonProperty("DataType")
        private String dataType;

        @JsonProperty("DisplayName")
        private String displayName;

        @JsonProperty("IsCustom")
        private boolean isCustom;

        @JsonProperty("Description")
        private String description;

        public String getColumnName() {
            return this.columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public String getDataType() {
            return this.dataType;
        }

        public void setDataType(String dataType) {
            this.dataType = dataType;
        }

        public String getDisplayName() {
            return this.displayName;
        }

        public void setDisplayName(String displayName) {
            this.displayName = displayName;
        }

        public boolean getIsCustom() {
            return this.isCustom;
        }

        public void setIsCustom(boolean isCustom) {
            this.isCustom = isCustom;
        }

        public String getDescription() {
            return this.description;
        }

        public void setDescription(String description) {
            this.description = description;
        }
    }
}
