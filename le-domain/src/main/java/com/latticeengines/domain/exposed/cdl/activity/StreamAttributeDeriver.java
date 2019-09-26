package com.latticeengines.domain.exposed.cdl.activity;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamAttributeDeriver implements Serializable {

    private static final long serialVersionUID = -2150789161039571407L;

    // target attribute to be generated
    @JsonProperty("target_attributes")
    private String targetAttribute;

    // input attributes to derive target attribute
    @JsonProperty("source_attributes")
    private List<String> sourceAttributes;

    // how to derive target attribute
    @JsonProperty("calculation")
    private Calculation calculation;

    public String getTargetAttribute() {
        return targetAttribute;
    }

    public void setTargetAttribute(String targetAttribute) {
        this.targetAttribute = targetAttribute;
    }

    public List<String> getSourceAttributes() {
        return sourceAttributes;
    }

    public void setSourceAttributes(List<String> sourceAttributes) {
        this.sourceAttributes = sourceAttributes;
    }

    public Calculation getCalculation() {
        return calculation;
    }

    public void setCalculation(Calculation calculation) {
        this.calculation = calculation;
    }

    public enum Calculation {
        // number of rows in the group
        COUNT("Count"), //
        // sum of input attribute (only accept single input attribute)
        SUM("Sum"), //
        // max value of input attribute (only accept single input attribute)
        MAX("Max"), //
        // min value of input attribute (only accept single input attribute)
        MIN("Min");

        private static Map<String, Calculation> nameMap;
        private static Set<String> values;

        static {
            nameMap = new HashMap<>();
            for (Calculation generator : Calculation.values()) {
                nameMap.put(generator.getName(), generator);
            }
            values = new HashSet<>(
                    Arrays.stream(values()).map(Calculation::name).collect(Collectors.toSet()));
        }

        private final String name;

        Calculation(String name) {
            this.name = name;
        }

        public static Calculation fromName(String name) {
            if (StringUtils.isBlank(name)) {
                return null;
            }
            if (values.contains(name.toUpperCase())) {
                return valueOf(name.toUpperCase());
            } else if (nameMap.containsKey(name)) {
                return nameMap.get(name);
            } else {
                throw new IllegalArgumentException("Cannot find a Calculation with name " + name);
            }
        }

        @Override
        public String toString() {
            return this.name;
        }

        public String getName() {
            return this.name;
        }    
    }
}
