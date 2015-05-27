package com.latticeengines.domain.exposed.modeling;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ModelingMetadata {
    
    private List<AttributeMetadata> attributeMetadata = new ArrayList<>();
    
    public static class KV {
        private String key;
        private String value;
        
        @JsonProperty("Key")
        public String getKey() {
            return key;
        }
        
        @JsonProperty("Key")
        public void setKey(String key) {
            this.key = key;
        }

        @JsonProperty("Value")
        public String getValue() {
            return value;
        }

        @JsonProperty("Value")
        public void setValue(String value) {
            this.value = value;
        }
        
        
    }


    public static class AttributeMetadata {
        private String approvedUsage;
        private String columnName;
        private String dataSource;
        private String dataType;
        private String description;
        private String displayDiscretizationStrategy;
        private String displayName;
        private List<KV> extensions;
        private String fundamentalType;
        private String lastTimeSourceUpdated;
        private String statisticalType;
        private List<String> tags;
        
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
        public List<KV> getExtensions() {
            return extensions;
        }

        @JsonProperty("Extensions")
        public void setExtensions(List<KV> extensions) {
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
        public List<String> getTags() {
            return tags;
        }

        @JsonProperty("Tags")
        public void setTags(List<String> tags) {
            this.tags = tags;
        }
        
    }

    @JsonProperty("Attributes")
    public List<AttributeMetadata> getAttributeMetadata() {
        return attributeMetadata;
    }

    @JsonProperty("Attributes")
    public void setAttributeMetadata(List<AttributeMetadata> attributeMetadata) {
        this.attributeMetadata = attributeMetadata;
    }

    
}
