package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BomboraSurgeConfig extends TransformerConfig {
    @JsonProperty("MetroAreaField")
    private String metroAreaField;

    @JsonProperty("DomainOriginField")
    private String domainOriginField;

    @JsonProperty("CountryField")
    private String countryField;

    @JsonProperty("StateField")
    private String stateField;

    @JsonProperty("CityField")
    private String cityField;

    @JsonProperty("CompoScoreField")
    private String compoScoreField;

    @JsonProperty("BucketCodeField")
    private String bucketCodeField;

    public String getMetroAreaField() {
        return metroAreaField;
    }

    public void setMetroAreaField(String metroAreaField) {
        this.metroAreaField = metroAreaField;
    }

    public String getDomainOriginField() {
        return domainOriginField;
    }

    public void setDomainOriginField(String domainOriginField) {
        this.domainOriginField = domainOriginField;
    }

    public String getCountryField() {
        return countryField;
    }

    public void setCountryField(String countryField) {
        this.countryField = countryField;
    }

    public String getStateField() {
        return stateField;
    }

    public void setStateField(String stateField) {
        this.stateField = stateField;
    }

    public String getCityField() {
        return cityField;
    }

    public void setCityField(String cityField) {
        this.cityField = cityField;
    }

    public String getCompoScoreField() {
        return compoScoreField;
    }

    public void setCompoScoreField(String compoScoreField) {
        this.compoScoreField = compoScoreField;
    }

    public String getBucketCodeField() {
        return bucketCodeField;
    }

    public void setBucketCodeField(String bucketCodeField) {
        this.bucketCodeField = bucketCodeField;
    }

}
