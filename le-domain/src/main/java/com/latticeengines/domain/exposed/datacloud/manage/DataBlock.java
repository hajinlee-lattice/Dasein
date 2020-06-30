package com.latticeengines.domain.exposed.datacloud.manage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

// a front-end object
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class DataBlock {

    private static final ImmutableMap<String, String> blockNameMap = ImmutableMap.<String, String>builder() //
            .put("companyfinancials", "Company Financials") //
            .put("companyinfo", "Company Information") //
            .put("diversityinsight", "Diversity Insights") //
            .put("eventfilings", "Filings and Events") //
            .put("financialstrengthinsight", "Financial Strength Insights") //
            .put("hierarchyconnections", "Hierarchies and Connections") //
            .put("ownershipinsight", "Ownership Insights") //
            .put("paymentinsight", "Payment Insights") //
            .put("principalscontacts", "Principal and Contacts") //
            .put("salesmarketinginsight", "Sales and Marketing Insights") //
            .put("thirdpartyriskinsight", "Third-Party Risk Insights") //
            .build();

    @JsonProperty("blockId")
    private String blockId;

    @JsonProperty("levels")
    private List<Level> levels;

    public DataBlock(String blockId, Collection<Level> levels) {
        this.blockId = blockId;
        this.levels = new ArrayList<>(levels);
    }

    public String getBlockId() {
        return blockId;
    }

    @JsonProperty("displayName")
    public String getBlockName() {
        return getBlockName(blockId);
    }

    public static String getBlockName(String blockId) {
        if (blockNameMap.containsKey(blockId)) {
            return blockNameMap.get(blockId);
        } else {
            throw new UnsupportedOperationException("Unknown block id " + blockId);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonAutoDetect( //
            fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE //
    )
    public static class Level {

        @JsonProperty("level")
        private DataBlockLevel level;

        @JsonProperty("description")
        private String description;

        @JsonProperty("elements")
        private List<Element> elements;

        public Level(DataBlockLevel level, String description, Collection<Element> elements) {
            this.level = level;
            this.description = description;
            this.elements = new ArrayList<>(elements);
        }

        public Level(DataBlockLevel level, String description) {
            this.level = level;
            this.description = description;
        }

        public DataBlockLevel getLevel() {
            return level;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonAutoDetect( //
            fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE //
    )
    public static class Element {

        @JsonProperty("elementId")
        private String elementId;

        @JsonProperty("displayName")
        private String displayName;

        public Element(PrimeColumn primeColumn) {
            this.elementId = primeColumn.getColumnId();
            this.displayName = primeColumn.getDisplayName();
        }

    }

}
