package com.latticeengines.domain.exposed.datacloud.transformation.config.source;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

public class DMXDataCleanConfig extends TransformerConfig {

    @JsonProperty("DomainField")
    private String domainField;
    
    @JsonProperty("DunsField")
    private String dunsField;

    @JsonProperty("VendorField")
    private String vendorField;

    @JsonProperty("ProductField")
    private String productField;

    @JsonProperty("CategoryField")
    private String categoryField;

    @JsonProperty("IntensityField")
    private String intensityField;
    
    @JsonProperty("DescriptionField")
    private String descriptionField;
    
    @JsonProperty("RecordValueField")
    private String recordValueField;
    
    @JsonProperty("RecordTypeField")
    private String recordTypeField;
    
    public String getDomainField() {
        return domainField;
    }

    public void setDomainField(String domainField) {
        this.domainField = domainField;
    }

    public String getDunsField() {
        return dunsField;
    }

    public void setDunsField(String dunsField) {
        this.dunsField = dunsField;
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

    public String getIntensityField() {
        return intensityField;
    }

    public void setIntensityField(String intensityField) {
        this.intensityField = intensityField;
    }
    
    public String getDescriptionField() {
        return descriptionField;
    }

    public void setDescriptionField(String descriptionField) {
        this.descriptionField = descriptionField;
    }
    
    public String getRecordValueField() {
        return recordValueField;
    }

    public void setRecordValueField(String recordValueField) {
        this.recordValueField = recordValueField;
    }
    
    public String getRecordTypeField() {
        return recordTypeField;
    }

    public void setRecordTypeField(String recordTypeField) {
        this.recordTypeField = recordTypeField;
    }

}
