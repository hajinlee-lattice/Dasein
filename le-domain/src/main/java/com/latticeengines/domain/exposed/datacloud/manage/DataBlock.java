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

    public static final class Id {
        protected Id() {
            throw new UnsupportedOperationException();
        }
        public static final String baseinfo = "baseinfo";
        public static final String entityresolution = "entityresolution";
        public static final String companyfinancials = "companyfinancials";
        public static final String companyinfo = "companyinfo";
        public static final String diversityinsight = "diversityinsight";
        public static final String eventfilings = "eventfilings";
        public static final String financialstrengthinsight = "financialstrengthinsight";
        public static final String hierarchyconnections = "hierarchyconnections";
        public static final String ownershipinsight = "ownershipinsight";
        public static final String paymentinsight = "paymentinsight";
        public static final String principalscontacts = "principalscontacts";
        public static final String salesmarketinginsight = "salesmarketinginsight";
        public static final String thirdpartyriskinsight = "thirdpartyriskinsight";
        public static final String businessactivityinsight = "businessactivityinsight";
        public static final String derivedtradeinsight = "dtri"; // Derived Trade Insight
        public static final String externaldisruptioninsight = "externaldisruptioninsight";
        public static final String inquiryinsight = "inquiryinsight";
        public static final String spendinsight = "spendinsight";
    }

    public static final String BLOCK_BASE_INFO = "baseinfo";
    public static final String BLOCK_ENTITY_RESOLUTION = "entityresolution";
    public static final String BLOCK_COMPANY_ENTITY_RESOLUTION = "companyentityresolution";

    public static final ImmutableMap<String, String> blockNameMap = ImmutableMap.<String, String>builder() //
            .put(Id.baseinfo, "Base Information") //
            .put(Id.entityresolution, "Company Entity Resolution") //
            .put(Id.companyfinancials, "Company Financials") //
            .put(Id.companyinfo, "Company Information") //
            .put(Id.diversityinsight, "Diversity Insights") //
            .put(Id.eventfilings, "Filings and Events") //
            .put(Id.financialstrengthinsight, "Financial Strength Insights") //
            .put(Id.hierarchyconnections, "Hierarchies and Connections") //
            .put(Id.ownershipinsight, "Ownership Insights") //
            .put(Id.paymentinsight, "Payment Insights") //
            .put(Id.principalscontacts, "Principal and Contacts") //
            .put(Id.salesmarketinginsight, "Sales and Marketing Insights") //
            .put(Id.thirdpartyriskinsight, "Third-Party Risk Insights") //
            .put(Id.businessactivityinsight, "Business Activity Insights") //
            .put(Id.derivedtradeinsight, "Derived Trade Insights") //  In the spreadsheet, LDC_ManageDB.DataBlockElement, and IDaaS API this is abbreviated dtri
            .put(Id.externaldisruptioninsight, "External Disruption Insights") //
            .put(Id.inquiryinsight, "Inquiry Insights") //
            .put(Id.spendinsight, "Spend Insights") //
            .build();

    @JsonProperty("blockId")
    private String blockId;

    @JsonProperty("levels")
    private List<Level> levels;

    // for jackson
    private DataBlock() {}

    public DataBlock(String blockId, Collection<Level> levels) {
        this.blockId = blockId;
        this.levels = new ArrayList<>(levels);
    }

    public String getBlockId() {
        return blockId;
    }

    public List<Level> getLevels() {
        return levels;
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

        // for jackson
        private Level() {}

        public Level(DataBlockLevel level, Collection<Element> elements) {
            this.level = level;
            this.elements = new ArrayList<>(elements);
        }

        public Level(DataBlockLevel level) {
            this.level = level;
        }

        public DataBlockLevel getLevel() {
            return level;
        }

        public List<Element> getElements() {
            return elements;
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

        @JsonProperty("description")
        private String description;

        @JsonProperty("dataType")
        private String dataType;

        @JsonProperty("example")
        private String example;

        // for jackson
        private Element() {}

        public Element(PrimeColumn primeColumn) {
            this.elementId = primeColumn.getColumnId();
            this.displayName = primeColumn.getDisplayName();
            this.description = primeColumn.getDescription();
            this.dataType = primeColumn.getJavaClass();
            this.example = primeColumn.getExample();
        }

        public String getElementId() {
            return this.elementId;
        }

        public String getDescription() {
            return this.description;
        }

        public String getDataType() {
            return dataType;
        }
    }

}
