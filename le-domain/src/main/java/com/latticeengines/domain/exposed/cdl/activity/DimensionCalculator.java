package com.latticeengines.domain.exposed.cdl.activity;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DimensionCalculator {
    // target attribute in stream to parse/calculate dimension
    @JsonProperty("attribute")
    private InterfaceName attribute;

    // how to parse/calculate dimension
    @JsonProperty("option")
    private DimensionCalculatorOption option;

    // for REGEX option: attribute in stream or catalog which contains regex
    // info
    @JsonProperty("pattern_attribute")
    private InterfaceName patternAttribute;

    // for REGEX option: whether the attribute which contains regex info
    // is from stream of catalog
    @JsonProperty("pattern_from_catalog")
    private boolean patternFromCatalog;

    public InterfaceName getAttribute() {
        return attribute;
    }

    public void setAttribute(InterfaceName attribute) {
        this.attribute = attribute;
    }

    public DimensionCalculatorOption getOption() {
        return option;
    }

    public void setOption(DimensionCalculatorOption option) {
        this.option = option;
    }

    public InterfaceName getPatternAttribute() {
        return patternAttribute;
    }

    public void setPatternAttribute(InterfaceName patternAttribute) {
        this.patternAttribute = patternAttribute;
    }

    public boolean isPatternFromCatalog() {
        return patternFromCatalog;
    }

    public void setPatternFromCatalog(boolean patternFromCatalog) {
        this.patternFromCatalog = patternFromCatalog;
    }

    public enum DimensionCalculatorOption {
        // for boolean/enum dimensions
        EXACT_MATCH("ExactMatch"),
        // for dimensions which need regex info to parse
        REGEX("Regex");

        private static Map<String, DimensionCalculatorOption> nameMap;
        private static Set<String> values;

        static {
            nameMap = new HashMap<>();
            for (DimensionCalculatorOption generator : DimensionCalculatorOption.values()) {
                nameMap.put(generator.getName(), generator);
            }
            values = new HashSet<>(
                    Arrays.stream(values()).map(DimensionCalculatorOption::name).collect(Collectors.toSet()));
        }

        private final String name;

        DimensionCalculatorOption(String name) {
            this.name = name;
        }

        public static DimensionCalculatorOption fromName(String name) {
            if (StringUtils.isBlank(name)) {
                return null;
            }
            if (values.contains(name.toUpperCase())) {
                return valueOf(name.toUpperCase());
            } else if (nameMap.containsKey(name)) {
                return nameMap.get(name);
            } else {
                throw new IllegalArgumentException("Cannot find a DimensionCalculatorOption with name " + name);
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
