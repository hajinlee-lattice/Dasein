package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BomboraSurgeCleanConfig extends TransformerConfig {
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

}
