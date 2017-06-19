package com.latticeengines.domain.exposed.scoringapi;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModelProperty;

@JsonInclude(Include.NON_NULL)
public class EnrichRequest {

    @JsonProperty("Domain")
    @ApiModelProperty(required = true, value = "Domain")
    private String domain;

    @JsonProperty("Company")
    @ApiModelProperty(value = "Company")
    private String company;

    @JsonProperty("State")
    @ApiModelProperty(value = "State")
    private String state;

    @JsonProperty("Country")
    @ApiModelProperty(value = "Country")
    private String country;

    @JsonProperty("source")
    @ApiModelProperty(value = "Name of the source system that originated this score request.")
    private String source;

    @JsonProperty("DUNS")
    @ApiModelProperty(value = "DUNS")
    private String duns;

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getDUNS() {
        return duns;
    }

    public void setDUNS(String duns) {
        this.duns = duns;
    }

}
