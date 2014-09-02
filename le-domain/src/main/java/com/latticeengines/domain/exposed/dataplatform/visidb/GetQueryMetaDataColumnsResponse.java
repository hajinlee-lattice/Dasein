package com.latticeengines.domain.exposed.dataplatform.visidb;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class GetQueryMetaDataColumnsResponse {

    private String errorMessage;
    private List<Metadata> metadata = new ArrayList<>();
    private int status;
    
    public static class Metadata {
        private String approvedUsage;
        private String columnName;
        private String dataSource;
        private String dataType;
        private String description;
        private String displayDiscretizationStrategy;
        private String displayName;
        private String extensions;
        private String fundamentalType;
        private String lastTimeSourceUpdated;
        private String statisticalType;
        private String tags;
        
        @JsonProperty("ApprovedUsage")
        public String getApprovedUsage() {
            return approvedUsage;
        }
        
        @JsonProperty("ApprovedUsage")
        public void setApprovedUsage(String approvedUsage) {
            this.approvedUsage = approvedUsage;
        }

        @JsonProperty("ColumnName")
        public String getColumnName() {
            return columnName;
        }

        @JsonProperty("ColumnName")
        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        @JsonProperty("DataSource")
        public String getDataSource() {
            return dataSource;
        }

        @JsonProperty("DataSource")
        public void setDataSource(String dataSource) {
            this.dataSource = dataSource;
        }

        @JsonProperty("DataType")
        public String getDataType() {
            return dataType;
        }

        @JsonProperty("DataType")
        public void setDataType(String dataType) {
            this.dataType = dataType;
        }

        @JsonProperty("Description")
        public String getDescription() {
            return description;
        }

        @JsonProperty("Description")
        public void setDescription(String description) {
            this.description = description;
        }

        @JsonProperty("DisplayDiscretizationStrategy")
        public String getDisplayDiscretizationStrategy() {
            return displayDiscretizationStrategy;
        }

        @JsonProperty("DisplayDiscretizationStrategy")
        public void setDisplayDiscretizationStrategy(String displayDiscretizationStrategy) {
            this.displayDiscretizationStrategy = displayDiscretizationStrategy;
        }

        @JsonProperty("DisplayName")
        public String getDisplayName() {
            return displayName;
        }

        @JsonProperty("DisplayName")
        public void setDisplayName(String displayName) {
            this.displayName = displayName;
        }

        @JsonProperty("Extensions")
        public String getExtensions() {
            return extensions;
        }

        @JsonProperty("Extensions")
        public void setExtensions(String extensions) {
            this.extensions = extensions;
        }

        @JsonProperty("FundamentalType")
        public String getFundamentalType() {
            return fundamentalType;
        }

        @JsonProperty("FundamentalType")
        public void setFundamentalType(String fundamentalType) {
            this.fundamentalType = fundamentalType;
        }

        @JsonProperty("LastTimeSourceUpdated")
        public String getLastTimeSourceUpdated() {
            return lastTimeSourceUpdated;
        }

        @JsonProperty("LastTimeSourceUpdated")
        public void setLastTimeSourceUpdated(String lastTimeSourceUpdated) {
            this.lastTimeSourceUpdated = lastTimeSourceUpdated;
        }

        @JsonProperty("StatisticalType")
        public String getStatisticalType() {
            return statisticalType;
        }

        @JsonProperty("StatisticalType")
        public void setStatisticalType(String statisticalType) {
            this.statisticalType = statisticalType;
        }

        @JsonProperty("Tags")
        public String getTags() {
            return tags;
        }

        @JsonProperty("Tags")
        public void setTags(String tags) {
            this.tags = tags;
        }
        
    }

    @JsonProperty("ErrorMessage")
    public String getErrorMessage() {
        return errorMessage;
    }

    @JsonProperty("ErrorMessage")
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @JsonProperty("Metadata")
    public List<Metadata> getMetadata() {
        return metadata;
    }

    @JsonProperty("Metadata")
    public void setMetadata(List<Metadata> metadata) {
        this.metadata = metadata;
    }

    @JsonProperty("Status")
    public int getStatus() {
        return status;
    }

    @JsonProperty("Status")
    public void setStatus(int status) {
        this.status = status;
    }
    
    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
