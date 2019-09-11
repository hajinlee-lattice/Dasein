package com.latticeengines.domain.exposed.metadata;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AttributeFixer {

    @JsonProperty("name")
    private String name;

    @JsonProperty("display_name")
    private String displayName;

    @JsonProperty("physical_data_type")
    private String physicalDataType;

    @JsonProperty("fundamental_type")
    private FundamentalType fundamentalType;

    @JsonProperty("statistical_type")
    private StatisticalType statisticalType;

    @JsonProperty("required")
    private Boolean required;

    @JsonProperty("date_format")
    private String dateFormat;

    @JsonProperty("time_format")
    private String timeFormat;

    @JsonProperty("timezone")
    private String timezone;

    @JsonProperty("validatorWrappers")
    private List<InputValidatorWrapper> validatorWrappers = new ArrayList<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getPhysicalDataType() {
        return physicalDataType;
    }

    public void setPhysicalDataType(String physicalDataType) {
        this.physicalDataType = physicalDataType;
    }

    public FundamentalType getFundamentalType() {
        return fundamentalType;
    }

    public void setFundamentalType(FundamentalType fundamentalType) {
        this.fundamentalType = fundamentalType;
    }

    public StatisticalType getStatisticalType() {
        return statisticalType;
    }

    public void setStatisticalType(StatisticalType statisticalType) {
        this.statisticalType = statisticalType;
    }

    public Boolean getRequired() {
        return required;
    }

    public void setRequired(Boolean required) {
        this.required = required;
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }

    public String getTimeFormat() {
        return timeFormat;
    }

    public void setTimeFormat(String timeFormat) {
        this.timeFormat = timeFormat;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public List<InputValidatorWrapper> getValidatorWrappers() {
        return validatorWrappers;
    }

    public void setValidatorWrappers(List<InputValidatorWrapper> validatorWrappers) {
        this.validatorWrappers = validatorWrappers;
    }

    @JsonIgnore
    public void copyToAttribute(Attribute attribute) {
        if (attribute == null) {
            return;
        }
        if (StringUtils.isNotEmpty(displayName)) {
            attribute.setDisplayName(displayName);
        }
        if (StringUtils.isNotEmpty(physicalDataType)) {
            attribute.setPhysicalDataType(physicalDataType);
        }
        if (StringUtils.isNotEmpty(dateFormat)) {
            attribute.setDateFormatString(dateFormat);
        }
        if (StringUtils.isNotEmpty(timeFormat)) {
            attribute.setTimeFormatString(timeFormat);
        }
        if (StringUtils.isNotEmpty(timezone)) {
            attribute.setTimezone(timezone);
        }
        if (fundamentalType != null) {
            attribute.setFundamentalType(fundamentalType);
        }
        if (statisticalType != null) {
            attribute.setStatisticalType(statisticalType);
        }
        if (required != null) {
            if (Boolean.TRUE.equals(required)) {
                attribute.setRequired(true);
            } else {
                attribute.setRequired(false);
            }
        }
        // here just need to check validatorWrappers is null as probably set empty back
        if (validatorWrappers != null) {
            attribute.setValidatorWrappers(validatorWrappers);
        }
    }
}
