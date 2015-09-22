package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class VdbMetadataField implements Cloneable {

    private String columnName;
    private String source;
    private String sourceToDisplay;
    private String category;
    private String displayName;
    private String approvedUsage;
    private String tags;
    private String fundamentalType;
    private String description;
    private String displayDiscretization;
    private String statisticalType;

    @JsonProperty("ColumnName")
    public String getColumnName() {
        return columnName;
    }

    @JsonProperty("ColumnName")
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    @JsonProperty("Source")
    public String getSource() {
        return source;
    }

    @JsonProperty("Source")
    public void setSource(String source) {
        this.source = source;
    }

    @JsonProperty("SourceToDisplay")
    public String getSourceToDisplay() {
        return sourceToDisplay;
    }

    @JsonProperty("SourceToDisplay")
    public void setSourceToDisplay(String sourceToDisplay) {
        this.sourceToDisplay = sourceToDisplay;
    }

    @JsonProperty("Category")
    public String getCategory() {
        return category;
    }

    @JsonProperty("Category")
    public void setCategory(String category) {
        this.category = category;
    }

    @JsonProperty("DisplayName")
    public String getDisplayName() {
        return displayName;
    }

    @JsonProperty("DisplayName")
    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @JsonProperty("ApprovedUsage")
    public String getApprovedUsage() {
        return approvedUsage;
    }

    @JsonProperty("ApprovedUsage")
    public void setApprovedUsage(String approvedUsage) {
        this.approvedUsage = approvedUsage;
    }

    @JsonProperty("Tags")
    public String getTags() {
        return tags;
    }

    @JsonProperty("Tags")
    public void setTags(String tags) {
        this.tags = tags;
    }

    @JsonProperty("FundamentalType")
    public String getFundamentalType() {
        return fundamentalType;
    }

    @JsonProperty("FundamentalType")
    public void setFundamentalType(String fundamentalType) {
        this.fundamentalType = fundamentalType;
    }

    @JsonProperty("Description")
    public String getDescription() {
        return description;
    }

    @JsonProperty("Description")
    public void setDescription(String description) {
        this.description = description;
    }

    @JsonProperty("DisplayDiscretization")
    public String getDisplayDiscretization() {
        return displayDiscretization;
    }

    @JsonProperty("DisplayDiscretization")
    public void setDisplayDiscretization(String displayDiscretization) {
        this.displayDiscretization = displayDiscretization;
    }

    @JsonProperty("StatisticalType")
    public String getStatisticalType() {
        return statisticalType;
    }

    @JsonProperty("StatisticalType")
    public void setStatisticalType(String statisticalType) {
        this.statisticalType = statisticalType;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((columnName == null) ? 0 : columnName.hashCode());
        result = prime * result + ((source == null) ? 0 : source.hashCode());
        result = prime * result + ((sourceToDisplay == null) ? 0 : sourceToDisplay.hashCode());
        result = prime * result + ((category == null) ? 0 : category.hashCode());
        result = prime * result + ((displayName == null) ? 0 : displayName.hashCode());
        result = prime * result + ((approvedUsage == null) ? 0 : approvedUsage.hashCode());
        result = prime * result + ((tags == null) ? 0 : tags.hashCode());
        result = prime * result + ((fundamentalType == null) ? 0 : fundamentalType.hashCode());
        result = prime * result + ((description == null) ? 0 : description.hashCode());
        result = prime * result + ((displayDiscretization == null) ? 0 : displayDiscretization.hashCode());
        result = prime * result + ((statisticalType == null) ? 0 : statisticalType.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass())
            return false;

        VdbMetadataField other = (VdbMetadataField)obj;
        if (!equals(getColumnName(), other.getColumnName())) {
            return false;
        }
        if (!equals(getSource(), other.getSource())) {
            return false;
        }
        if (!equals(getSourceToDisplay(), other.getSourceToDisplay())) {
            return false;
        }
        if (!equals(getDisplayName(), other.getDisplayName())) {
            return false;
        }
        if (!equals(getTags(), other.getTags())) {
            return false;
        }
        if (!equals(getCategory(), other.getCategory())) {
            return false;
        }
        if (!equals(getApprovedUsage(), other.getApprovedUsage())) {
            return false;
        }
        if (!equals(getFundamentalType(), other.getFundamentalType())) {
            return false;
        }
        if (!equals(getDescription(), other.getDescription())) {
            return false;
        }
        if (!equals(getDisplayDiscretization(), other.getDisplayDiscretization())) {
            return false;
        }
        if (!equals(getStatisticalType(), other.getStatisticalType())) {
            return false;
        }

        return true;
    }

    private boolean equals(String str1, String str2) {
        if (str1 == null) {
            return str2 == null;
        }
        return str1.equals(str2);
    }

    @Override
    public Object clone()
    {
        VdbMetadataField field = new VdbMetadataField();
        field.setColumnName(getColumnName());
        field.setSource(getSource());
        field.setSourceToDisplay(getSourceToDisplay());
        field.setDisplayName(getDisplayName());
        field.setTags(getTags());
        field.setCategory(getCategory());
        field.setApprovedUsage(getApprovedUsage());
        field.setFundamentalType(getFundamentalType());
        field.setDescription(getDescription());
        field.setDisplayDiscretization(getDisplayDiscretization());
        field.setStatisticalType(getStatisticalType());
        return field;
    }

}
