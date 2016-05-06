package com.latticeengines.domain.exposed.modeling;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.AllowedValues;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.common.exposed.validator.annotation.RequiredKeysInMap;

public class ModelingMetadata {

    public static final String NONE_APPROVED_USAGE = "None";
    public static final String MODEL_APPROVED_USAGE = "Model";
    public static final String MODEL_AND_MODEL_INSIGHTS_APPROVED_USAGE = "ModelAndModelInsights";
    public static final String MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE = "ModelAndAllInsights";
    public static final String INTERNAL_TAG = "Internal";
    public static final String EXTERNAL_TAG = "External";
    public static final String INTERNAL_TRANSFORM_TAG = "InternalTransform";
    public static final String EXTERNAL_TRANSFORM_TAG = "ExternalTransform";
    public static final String NOMINAL_STAT_TYPE = "nominal";
    public static final String ORDINAL_STAT_TYPE = "ordinal";
    public static final String INTERVAL_STAT_TYPE = "interval";
    public static final String RATIO_STAT_TYPE = "ratio";
    public static final String CATEGORY_EXTENSION = "Category";
    public static final String CATEGORY_LEAD_INFORMATION = "Lead Information";
    public static final String FT_ALPHA = "alpha";
    public static final String FT_BOOLEAN = "boolean";
    public static final String FT_CURRENCY = "currency";
    public static final String FT_NUMERIC = "numeric";
    public static final String FT_PERCENTAGE = "percentage";
    public static final String FT_YEAR = "year";
    public static final String FT_EMAIL = "email";
    public static final String FT_PROBABILITY = "probability";
    public static final String FT_PHONE = "phone";
    public static final String FT_URI = "uri";
    public static final String FT_ENUM = "enum";

    private List<AttributeMetadata> attributeMetadata = new ArrayList<>();

    public static class KV implements Map.Entry<String, String> {
        private String key;
        private String value;

        public KV() {
        }

        public KV(String key, String value) {
            this.key = key;
            this.value = value;
        }

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
        public String setValue(String value) {
            String oldValue = this.value;
            this.value = value;
            return oldValue;
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

        public String toString() {
            return "{\"DateTime\":\"" + dateTime + "\",\"OffsetMinutes\":" + offsetMinutes + "}";
        }
    }

    public static class AttributeMetadata {
        @AllowedValues(values = { //
        MODEL_APPROVED_USAGE, //
                MODEL_AND_MODEL_INSIGHTS_APPROVED_USAGE, //
                MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE, //
                NONE_APPROVED_USAGE })
        @NotNull
        private List<String> approvedUsage;
        private String columnName;
        private List<String> dataSource;
        private String dataType;
        private String description;
        private String displayDiscretizationStrategy;
        @NotEmptyString
        @NotNull
        private String displayName;
        @RequiredKeysInMap(keys = { CATEGORY_EXTENSION })
        private List<KV> extensions;
        @AllowedValues(values = { FT_ALPHA, FT_BOOLEAN, FT_CURRENCY, FT_NUMERIC, FT_PERCENTAGE, FT_YEAR, FT_EMAIL,
                FT_PROBABILITY, FT_PHONE, FT_URI, FT_ENUM })
        private String fundamentalType;
        private List<DateTime> lastTimeSourceUpdated;
        private DateTime mostRecentUpdateDate;
        @AllowedValues(values = { NOMINAL_STAT_TYPE, ORDINAL_STAT_TYPE, INTERVAL_STAT_TYPE, RATIO_STAT_TYPE })
        @NotNull
        private String statisticalType;
        @AllowedValues(values = { INTERNAL_TAG, EXTERNAL_TAG })
        @NotNull
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

    @JsonProperty("Metadata")
    public List<AttributeMetadata> getAttributeMetadata() {
        return attributeMetadata;
    }

    @JsonProperty("Metadata")
    public void setAttributeMetadata(List<AttributeMetadata> attributeMetadata) {
        this.attributeMetadata = attributeMetadata;
    }

}
