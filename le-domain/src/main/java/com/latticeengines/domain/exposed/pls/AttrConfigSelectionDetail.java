package com.latticeengines.domain.exposed.pls;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AttrConfigSelectionDetail {

    @JsonProperty("Selected")
    private Long selected;

    @JsonProperty("Limit")
    private Long limit;

    @JsonProperty("TotalAttrs")
    private Long totalAttrs;

    @JsonProperty("Subcategories")
    private List<SubcategoryDetail> subcategories;

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

    public List<SubcategoryDetail> getSubcategories() {
        return this.subcategories;
    }

    public void setSubcategories(List<SubcategoryDetail> subcategories) {
        this.subcategories = subcategories;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SubcategoryDetail {

        @JsonProperty("DisplayName")
        private String subCategory;

        @JsonProperty("Selected")
        private Boolean selected = Boolean.FALSE;

        @JsonProperty("TotalAttrs")
        private Long totalAttrs = 0L;

        @JsonProperty("HasFrozenAttrs")
        private Boolean hasFrozenAttrs = Boolean.FALSE;

        @JsonProperty("Attributes")
        private List<AttrDetail> attributes = new ArrayList<>();

        public String getSubCategory() {
            return subCategory;
        }

        public void setSubCategory(String subCategoryVal) {
            subCategory = subCategoryVal;
        }

        public Boolean getSelected() {
            return selected;
        }

        public void setSelected(Boolean selectedValue) {
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

        public List<AttrDetail> getAttributes() {
            return attributes;
        }

        public void setAttributes(List<AttrDetail> attributesValue) {
            attributes = attributesValue;
        }

    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AttrDetail {

        @JsonProperty("Attribute")
        private String attribute;

        @JsonProperty("DisplayName")
        private String displayName;

        @JsonProperty("DefaultName")
        private String defaultName;

        @JsonProperty("Description")
        private String description;

        @JsonProperty("Selected")
        private Boolean selected;

        @JsonProperty("IsFrozen")
        private Boolean isFrozen;

        public String getAttribute() {
            return attribute;
        }

        public void setAttribute(String attributeVal) {
            attribute = attributeVal;
        }

        public String getDisplayName() {
            return displayName;
        }

        public void setDisplayName(String displayNameValue) {
            displayName = displayNameValue;
        }

        public String getDefaultName() {
            return defaultName;
        }

        public void setDefaultName(String defaultNameVal) {
            defaultName = defaultNameVal;
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

        @Override
        public String toString() {
            return JsonUtils.serialize(this);
        }
    }
}
