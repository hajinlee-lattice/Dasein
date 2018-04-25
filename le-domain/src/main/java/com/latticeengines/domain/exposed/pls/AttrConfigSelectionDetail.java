package com.latticeengines.domain.exposed.pls;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AttrConfigSelectionDetail {

    @JsonProperty("Selected")
    private Long selected;

    @JsonProperty("Limit")
    private Long limit;

    @JsonProperty("TotalAttrs")
    private Long totalAttrs;

    @JsonProperty("Entity")
    private BusinessEntity entity;

    @JsonProperty("Subcategories")
    private Map<String, SubcategoryDetail> subcategories;

    public Long getSelected() {
        return this.selected;
    }

    public void setSelected(Long selected) {
        this.selected = selected;
    }

    public Long getLimit() {
        return this.limit;
    }

    public void setLimit(Long limit) {
        this.limit = limit;
    }

    public Long getTotalAttrs() {
        return this.totalAttrs;
    }

    public void setTotalAttrs(Long totalAttrs) {
        this.totalAttrs = totalAttrs;
    }

    public BusinessEntity getEntity() {
        return this.entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }

    public Map<String, SubcategoryDetail> getSubcategories() {
        return this.subcategories;
    }

    public void setSubcategories(Map<String, SubcategoryDetail> subcategories) {
        this.subcategories = subcategories;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SubcategoryDetail {

        @JsonProperty("Selected")
        private Long selected;

        @JsonProperty("TotalAttrs")
        private Long totalAttrs;

        @JsonProperty("HasFrozenAttrs")
        private Boolean hasFrozenAttrs;

        @JsonProperty("Attributes")
        private Map<String, AttrDetail> attributes;

        public Long getSelected() {
            return selected;
        }

        public void setSelected(Long selectedValue) {
            selected = selectedValue;
        }

        public Long getTotalAttrs() {
            return totalAttrs;
        }

        public void setTotalAttrs(Long totalAttrsValue) {
            totalAttrs = totalAttrsValue;
        }

        public Boolean getHasFrozenAttrs() {
            return hasFrozenAttrs;
        }

        public void setHasFrozenAttrs(Boolean hasFrozenAttrsValue) {
            hasFrozenAttrs = hasFrozenAttrsValue;
        }

        public Map<String, AttrDetail> getAttributes() {
            return attributes;
        }

        public void setAttributes(Map<String, AttrDetail> attributesValue) {
            attributes = attributesValue;
        }

    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AttrDetail {

        @JsonProperty("DisplayName")
        private String displayName;

        @JsonProperty("Description")
        private String description;

        @JsonProperty("selected")
        private Boolean selected;

        @JsonProperty("IsFrozen")
        private Boolean isFrozen;

        public String getDisplayName() {
            return displayName;
        }

        public void setDisplayName(String displayNameValue) {
            displayName = displayNameValue;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String descriptionValue) {
            description = descriptionValue;
        }

        public Boolean getSelected() {
            return selected;
        }

        public void setSelected(Boolean selectedValue) {
            selected = selectedValue;
        }

        public Boolean getIsFrozen() {
            return isFrozen;
        }

        public void setIsFrozen(Boolean isFrozenValue) {
            isFrozen = isFrozenValue;
        }
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
