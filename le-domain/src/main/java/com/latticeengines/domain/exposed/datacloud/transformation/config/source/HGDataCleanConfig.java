package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;

public class HGDataCleanConfig extends TransformerConfig {

    @JsonProperty("FakedCurrentDate")
    private Date fakedCurrentDate;

    @JsonProperty("DomainField")
    private String domainField;

    @JsonProperty("DateLastVerifiedField")
    private String dateLastVerifiedField;

    @JsonProperty("VendorField")
    private String vendorField;

    @JsonProperty("ProductField")
    private String productField;

    @JsonProperty("CategoryField")
    private String categoryField;

    @JsonProperty("CategoryParentField")
    private String categoryParentField;

    @JsonProperty("Category2Field")
    private String category2Field;

    @JsonProperty("CategoryParent2Field")
    private String categoryParent2Field;

    @JsonProperty("IntensityField")
    private String intensityField;

    public Date getFakedCurrentDate() {
        return fakedCurrentDate;
    }

    public void setFakedCurrentDate(Date fakedCurrentDate) {
        this.fakedCurrentDate = fakedCurrentDate;
    }

    public String getDomainField() {
        return domainField;
    }

    public void setDomainField(String domainField) {
        this.domainField = domainField;
    }

    public String getDateLastVerifiedField() {
        return dateLastVerifiedField;
    }

    public void setDateLastVerifiedField(String dateLastVerifiedField) {
        this.dateLastVerifiedField = dateLastVerifiedField;
    }

    public String getVendorField() {
        return vendorField;
    }

    public void setVendorField(String vendorField) {
        this.vendorField = vendorField;
    }

    public String getProductField() {
        return productField;
    }

    public void setProductField(String productField) {
        this.productField = productField;
    }

    public String getCategoryField() {
        return categoryField;
    }

    public void setCategoryField(String categoryField) {
        this.categoryField = categoryField;
    }

    public String getCategoryParentField() {
        return categoryParentField;
    }

    public void setCategoryParentField(String categoryParentField) {
        this.categoryParentField = categoryParentField;
    }

    public String getCategory2Field() {
        return category2Field;
    }

    public void setCategory2Field(String category2Field) {
        this.category2Field = category2Field;
    }

    public String getCategoryParent2Field() {
        return categoryParent2Field;
    }

    public void setCategoryParent2Field(String categoryParent2Field) {
        this.categoryParent2Field = categoryParent2Field;
    }

    public String getIntensityField() {
        return intensityField;
    }

    public void setIntensityField(String intensityField) {
        this.intensityField = intensityField;
    }

}
