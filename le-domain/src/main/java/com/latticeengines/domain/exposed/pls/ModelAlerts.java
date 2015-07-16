package com.latticeengines.domain.exposed.pls;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ModelAlerts {

    private ModelQualityWarnings modelQualityWarnings;

    @JsonProperty("ModelQualityWarnings")
    public ModelQualityWarnings getModelQualityWarnings() { return modelQualityWarnings; }

    @JsonProperty("ModelQualityWarnings")
    public void setModelQualityWarnings(ModelQualityWarnings modelQualityWarnings) {
        this.modelQualityWarnings = modelQualityWarnings;
    }

    public static class ModelQualityWarnings {

        boolean success;
        List<AttributeValuePair> lowConversionRateAttributes = new ArrayList<>();

        @JsonProperty("Success")
        public boolean isSuccess() { return success; }

        @JsonProperty("Success")
        public void setSuccess(boolean success) { this.success = success; }

        @JsonProperty("LowConversionRateAttributes")
        public List<AttributeValuePair> getLowConversionRateAttributes() { return lowConversionRateAttributes; }

        @JsonProperty("LowConversionRateAttributes")
        public void setLowConversionRateAttributes(List<AttributeValuePair> attributes) {
            this.lowConversionRateAttributes = attributes;
        }

    }

    public static class AttributeValuePair {
        private String attribute;
        private String value;

        // empty constructor for serialization
        public AttributeValuePair() { }

        public AttributeValuePair(String attribute, String value) {
            this.setAttribute(attribute);
            this.setValue(value);
        }

        @JsonProperty("Attribute")
        public String getAttribute() { return attribute; }

        @JsonProperty("Attribute")
        public void setAttribute(String attribute) { this.attribute = attribute; }

        @JsonProperty("Value")
        public String getValue() { return value; }

        @JsonProperty("Value")
        public void setValue(String value) { this.value = value; }

    }

}
