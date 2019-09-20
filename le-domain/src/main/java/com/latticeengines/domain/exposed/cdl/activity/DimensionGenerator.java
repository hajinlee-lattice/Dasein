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

/**
 * This class is to configure how to generate activity dimension value universe
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DimensionGenerator {
    // attribute to get/generate dimension values, could be from stream of
    // catalog
    @JsonProperty("attribute")
    private InterfaceName attribute;

    // whether the attribute to get/generate dimension values is from stream of
    // catalog
    @JsonProperty("from_catalog")
    private boolean fromCatalog;

    // how to get/generate dimension values
    @JsonProperty("option")
    private DimensionGeneratorOption option;
    
    public InterfaceName getAttribute() {
        return attribute;
    }

    public void setAttribute(InterfaceName attribute) {
        this.attribute = attribute;
    }

    public boolean isFromCatalog() {
        return fromCatalog;
    }

    public void setFromCatalog(boolean fromCatalog) {
        this.fromCatalog = fromCatalog;
    }

    public DimensionGeneratorOption getOption() {
        return option;
    }

    public void setOption(DimensionGeneratorOption option) {
        this.option = option;
    }

    public enum DimensionGeneratorOption {
        // Use case: WebVisitPathPattern -- take pattern names provided in
        // WebVisitPathPattern catalog, standardize and calculate hash strings
        // as dimension values
        HASH("Hash"), 
        // Use case 1: MarketoActivity -- take ActivityTypeId attribute values
        // provided in MarketoActivityTypes catalog as dimension values
        // Use case 2: EloquaActivity -- take ActivityType attribute values
        // provided in EloquaActivity stream, choose limited number of them as
        // dimension values
        ENUM("Enum"),
        // Use case: Opportunity -- IsWon is a boolean attribute, simply take
        // true and false as dimension values
        BOOLEAN("Boolean");

        private static Map<String, DimensionGeneratorOption> nameMap;
        private static Set<String> values;

        static {
            nameMap = new HashMap<>();
            for (DimensionGeneratorOption generator : DimensionGeneratorOption.values()) {
                nameMap.put(generator.getName(), generator);
            }
            values = new HashSet<>(
                    Arrays.stream(values()).map(DimensionGeneratorOption::name).collect(Collectors.toSet()));
        }

        private final String name;

        DimensionGeneratorOption(String name) {
            this.name = name;
        }

        public static DimensionGeneratorOption fromName(String name) {
            if (StringUtils.isBlank(name)) {
                return null;
            }
            if (values.contains(name.toUpperCase())) {
                return valueOf(name.toUpperCase());
            } else if (nameMap.containsKey(name)) {
                return nameMap.get(name);
            } else {
                throw new IllegalArgumentException("Cannot find a DimensionGeneratorOption with name " + name);
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
