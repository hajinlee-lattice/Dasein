package com.latticeengines.domain.exposed.metadata;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * provide a collection of options for tag filtering and associated metadata
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class FilterOptions {

    @JsonProperty("Label")
    private String label;

    @JsonProperty("Options")
    private List<Option> options;

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public List<Option> getOptions() {
        return options;
    }

    public void setOptions(List<Option> options) {
        this.options = options;
    }

    @Override
    public String toString() {
        return "FilterOptions{" + "label='" + label + '\'' + ", options=" + options + '}';
    }

    /*
     * metadata for single filter option
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class Option {
        public static final String ANY_DISPLAY_NAME = "Any";
        public static final String ANY_VALUE = "any";

        public static Option anyAttrOption() {
            Option option = new Option();
            option.setDisplayName(ANY_DISPLAY_NAME);
            option.setValue(ANY_VALUE);
            return option;
        }

        @JsonProperty("DisplayName")
        private String displayName;

        @JsonProperty("Value")
        private String value;

        public String getDisplayName() {
            return displayName;
        }

        public void setDisplayName(String displayName) {
            this.displayName = displayName;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "Option{" + "displayName='" + displayName + '\'' + ", value='" + value + '\'' + '}';
        }
    }
}
