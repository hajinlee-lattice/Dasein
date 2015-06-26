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
    
    public static class DateTime {
        private String dateTime;
        private int offsetMinutes;
        
        @JsonProperty("DateTime")
        public String getDateTime() {
            return dateTime;
        }
        
        @JsonProperty("DateTime")
        public void setDateTime(String dateTime) {
            this.dateTime = dateTime;
        }

        @JsonProperty("OffsetMinutes")
        public int getOffsetMinutes() {
            return offsetMinutes;
        }

        @JsonProperty("OffsetMinutes")
        public void setOffsetMinutes(int offsetMinutes) {
            this.offsetMinutes = offsetMinutes;
        }
        
        public String toString(){
            return "{\"DateTime\":\"" + dateTime + "\",\"OffsetMinutes\":" + offsetMinutes+"}";
        }
    }


    public static class AttributeMetadata {
        private List<String> approvedUsage;
        private String columnName;
        private List<String> dataSource;
        private String dataType;
        private String description;
        private String displayDiscretizationStrategy;
        private String displayName;
        private List<KV> extensions;
        private String fundamentalType;
        private List<DateTime> lastTimeSourceUpdated;
        private DateTime mostRecentUpdateDate;
        private String statisticalType;
        private List<String> tags;
        private String dataQuality;
        
        @JsonProperty("ApprovedUsage")
        public List<String> getApprovedUsage() {
            return approvedUsage;
        }
        
        @JsonProperty("ApprovedUsage")
        public void setApprovedUsage(List<String> approvedUsage) {
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
        public List<String> getDataSource() {
            return dataSource;
        }

        @JsonProperty("DataSource")
        public void setDataSource(List<String> dataSource) {
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
        public List<DateTime> getLastTimeSourceUpdated() {
            return lastTimeSourceUpdated;
        }

        @JsonProperty("LastTimeSourceUpdated")
        public void setLastTimeSourceUpdated(List<DateTime> lastTimeSourceUpdated) {
            this.lastTimeSourceUpdated = lastTimeSourceUpdated;
        }

        @JsonProperty("MostRecentUpdateDate")
        public DateTime getMostRecentUpdateDate() {
            return mostRecentUpdateDate;
        }

        @JsonProperty("MostRecentUpdateDate")
        public void setMostRecentUpdateDate(DateTime mostRecentUpdateDate) {
            this.mostRecentUpdateDate = mostRecentUpdateDate;
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

        @JsonProperty("DataQuality")
        public String getDataQuality() {
            return dataQuality;
        }

        @JsonProperty("DataQuality")
        public void setDataQuality(String dataQuality) {
            this.dataQuality = dataQuality;
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
