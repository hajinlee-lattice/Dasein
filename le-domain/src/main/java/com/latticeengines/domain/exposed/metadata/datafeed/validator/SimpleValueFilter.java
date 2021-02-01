package com.latticeengines.domain.exposed.metadata.datafeed.validator;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.parquet.Strings;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A very simple value filter
 * All restrictions are linked with 'and'
 * Flag reverse = True means skip the record if meets restrictions ELSE only accept record that meets restriction.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class SimpleValueFilter extends TemplateValidator {

    @JsonProperty("reverse")
    private Boolean reverse;

    @JsonProperty("restrictions")
    private List<Restriction> restrictions;

    public Boolean getReverse() {
        return reverse;
    }

    public void setReverse(Boolean reverse) {
        this.reverse = reverse;
    }

    public List<Restriction> getRestrictions() {
        return restrictions;
    }

    public void setRestrictions(List<Restriction> restrictions) {
        this.restrictions = restrictions;
    }

    @Override
    public boolean accept(Map<String, String> record, Map<String, String> errorMsg) {
        if (CollectionUtils.isEmpty(restrictions)) {
            return true;
        }
        boolean accept = true;
        for (Restriction restriction : restrictions) {
            accept = accept && restriction.meetRestriction(record);
        }
        if (Boolean.TRUE.equals(reverse) ^ accept) {
            return true;
        } else {
            List<String> messages = restrictions.stream().map(Restriction::toReadableString).collect(Collectors.toList());
            String error;
            if (Boolean.TRUE.equals(reverse)) {
                error = "Skip if meets: " + Strings.join(messages, " AND ");
            } else {
                error = "Does not meet: " + Strings.join(messages, " AND ");
            }
            errorMsg.put("Error: ", error);
            return false;
        }
    }

    public static class Restriction {

        @JsonProperty("field_name")
        private String fieldName;

        @JsonProperty("value")
        private String value;

        @JsonProperty("operator")
        private Operator operator;

        public String getFieldName() {
            return fieldName;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public Operator getOperator() {
            return operator;
        }

        public void setOperator(Operator operator) {
            this.operator = operator;
        }

        @JsonIgnore
        public String toReadableString() {
            if (Operator.EQUAL.equals(operator)) {
                return String.format("%s = '%s'", fieldName, value);
            } else {
                return String.format("%s != '%s'", fieldName, value);
            }
        }

        @JsonIgnore
        public boolean meetRestriction(Map<String, String> record) {
            if (record.get(fieldName) == null) {
                return false;
            } else {
                switch (operator) {
                    case EQUAL:
                        return record.get(fieldName).equals(value);
                    case NOT_EQUAL:
                        return !record.get(fieldName).equals(value);
                    case MATCH:
                        return record.get(fieldName).matches(value);
                    default:
                        return false;
                }
            }
        }

        public enum Operator {
            EQUAL,
            NOT_EQUAL,
            MATCH
        }
    }
}
