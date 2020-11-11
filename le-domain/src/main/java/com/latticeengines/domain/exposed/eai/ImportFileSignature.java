package com.latticeengines.domain.exposed.eai;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ImportFileSignature {

    @JsonProperty("has_duns")
    private Boolean hasDUNS = false;

    @JsonProperty("has_domain")
    private Boolean hasDomain = false;

    @JsonProperty("has_company_name")
    private Boolean hasCompanyName = false;

    public Boolean getHasDUNS() {
        return hasDUNS;
    }

    public void setHasDUNS(Boolean hasDUNS) {
        this.hasDUNS = hasDUNS;
    }

    public Boolean getHasDomain() {
        return hasDomain;
    }

    public void setHasDomain(Boolean hasDomain) {
        this.hasDomain = hasDomain;
    }

    public Boolean getHasCompanyName() {
        return hasCompanyName;
    }

    public void setHasCompanyName(Boolean hasCompanyName) {
        this.hasCompanyName = hasCompanyName;
    }
}
