package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class AttrConfigLifeCycleChangeConfiguration extends ActionConfiguration {

    @JsonProperty("CategoryName")
    private String categoryName;
    @JsonProperty("subType")
    private SubType subType;
    @JsonProperty("AttrNums")
    private Long attrNums;

    public AttrConfigLifeCycleChangeConfiguration() {
    }

    public String getCategoryName() {
        return this.categoryName;
    }

    public void setCategoryName(String categoryName) {
        this.categoryName = categoryName;
    }

    public SubType getSubType() {
        return this.subType;
    }

    public void setSubType(SubType subType) {
        this.subType = subType;
    }

    public Long getAttrNums() {
        return this.attrNums;
    }

    public void setAttrNums(Long attrNums) {
        this.attrNums = attrNums;
    }

    @Override
    public String serialize() {
        return String.format(subType.getFormat(), attrNums);
    }

    public enum SubType {

        ACTIVATION("%d Attributes Activated"), //
        DEACTIVATION("%d Attributes Deactivated");

        private String format;

        SubType(String format) {
            this.format = format;
        }

        public String getFormat() {
            return this.format;
        }
    }

}
