package com.latticeengines.domain.exposed.cdl;

import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.query.EntityType;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class SimpleTemplateMetadata {

    @NotNull
    @JsonProperty("entity_type")
    private EntityType entityType;

    @JsonProperty("ignored_standard_attributes")
    private Set<String> ignoredStandardAttributes;

    @JsonProperty("standard_attributes")
    private List<SimpleTemplateAttribute> standardAttributes;

    @JsonProperty("customer_attributes")
    private List<SimpleTemplateAttribute> customerAttributes;

    public EntityType getEntityType() {
        return entityType;
    }

    public void setEntityType(EntityType entityType) {
        this.entityType = entityType;
    }

    public List<SimpleTemplateAttribute> getStandardAttributes() {
        return standardAttributes;
    }

    public void setStandardAttributes(List<SimpleTemplateAttribute> standardAttributes) {
        this.standardAttributes = standardAttributes;
    }

    public Set<String> getIgnoredStandardAttributes() {
        return ignoredStandardAttributes;
    }

    public void setIgnoredStandardAttributes(Set<String> ignoredStandardAttributes) {
        this.ignoredStandardAttributes = ignoredStandardAttributes;
    }

    public List<SimpleTemplateAttribute> getCustomerAttributes() {
        return customerAttributes;
    }

    public void setCustomerAttributes(List<SimpleTemplateAttribute> customerAttributes) {
        this.customerAttributes = customerAttributes;
    }

    public static class SimpleTemplateAttribute {

        @NotNull
        @JsonProperty("display_name")
        private String displayName;

        @JsonProperty("name")
        private String name;

        @NotNull
        @JsonProperty("physical_data_type")
        private Schema.Type physicalDataType;

        @JsonProperty("approved_usages")
        private List<String> approvedUsages;

        @JsonProperty("fundamental_type")
        private String fundamentalType;

        public String getDisplayName() {
            return displayName;
        }

        public void setDisplayName(String displayName) {
            this.displayName = displayName;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Schema.Type getPhysicalDataType() {
            return physicalDataType;
        }

        public void setPhysicalDataType(Schema.Type physicalDataType) {
            this.physicalDataType = physicalDataType;
        }

        public List<String> getApprovedUsages() {
            return approvedUsages;
        }

        public void setApprovedUsage(List<String> approvedUsages) {
            this.approvedUsages = approvedUsages;
        }

        public String getFundamentalType() {
            return fundamentalType;
        }

        public void setFundamentalType(String fundamentalType) {
            this.fundamentalType = fundamentalType;
        }
    }
}
