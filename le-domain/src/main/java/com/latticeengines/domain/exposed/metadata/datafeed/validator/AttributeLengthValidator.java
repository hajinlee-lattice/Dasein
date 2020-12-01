package com.latticeengines.domain.exposed.metadata.datafeed.validator;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class AttributeLengthValidator extends TemplateValidator {

    @JsonProperty("attribute_name")
    private String attributeName;

    @JsonProperty("nullable")
    private Boolean nullable;

    @JsonProperty("length")
    private Integer length;

    public String getAttributeName() {
        return attributeName;
    }

    public void setAttributeName(String attributeName) {
        this.attributeName = attributeName;
    }

    public Boolean getNullable() {
        return nullable;
    }

    public void setNullable(Boolean nullable) {
        this.nullable = nullable;
    }

    public Integer getLength() {
        return length;
    }

    public void setLength(Integer length) {
        this.length = length;
    }

    @Override
    public boolean accept(Map<String, String> record, Map<String, String> errorMsg) {
        if (record.containsKey(attributeName)) {
            String value = record.get(attributeName);
            if (value == null) {
                if (!Boolean.TRUE.equals(nullable)) {
                    errorMsg.put(attributeName, attributeName + " cannot be null!");
                }
                return Boolean.TRUE.equals(nullable);
            } else {
                if (length == null) {
                    return true;
                }
                if (StringUtils.length(value) == length) {
                    return true;
                } else {
                    errorMsg.put(attributeName, String.format("Value length(%d) does not meet required length(%d)",
                            value.length(), length));
                    return false;
                }
            }
        } else {
            if (!Boolean.TRUE.equals(nullable)) {
                errorMsg.put(attributeName, attributeName + " cannot be null!");
            }
            return Boolean.TRUE.equals(nullable);
        }
    }
}
