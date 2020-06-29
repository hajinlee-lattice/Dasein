package com.latticeengines.domain.exposed.datacloud.manage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

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
        String blockName;
        switch (blockId.toLowerCase()) {
            case "companyfinancials":
                blockName = "Company Financials";
                break;
            case "companyinfo":
                blockName = "Company Information";
                break;
            case "diversityinsight":
                blockName = "Diversity Insights";
                break;
            case "eventfilings":
                blockName = "Filings & Events";
                break;
            case "financialstrengthinsight":
                blockName = "Financial Strength Insights";
                break;
            case "hierarchyconnections":
                blockName = "Hierarchies & Connections";
                break;
            case "ownershipinsight":
                blockName = "Ownership Insights";
                break;
            case "paymentinsight":
                blockName = "Payment Insights";
                break;
            case "principalscontacts":
                blockName = "Principal & Contacts";
                break;
            case "salesmarketinginsight":
                blockName = "Sales & Marketing Insights";
                break;
            case "thirdpartyriskinsight":
                blockName = "Third-Party Risk Insights";
                break;
            default:
                throw new UnsupportedOperationException("Unknown block id " + blockId);
        }
        return blockName;
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
