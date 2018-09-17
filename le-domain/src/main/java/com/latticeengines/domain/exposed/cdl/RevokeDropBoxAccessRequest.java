package com.latticeengines.domain.exposed.cdl;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class RevokeDropBoxAccessRequest {

    @JsonProperty("AccessMode")
    private DropBoxAccessMode accessMode;

    @JsonProperty("LatticeUser")
    private String latticeUser;

    @JsonProperty("ExternalAccountId")
    private String externalAccountId;

    public DropBoxAccessMode getAccessMode() {
        return accessMode;
    }

    public void setAccessMode(DropBoxAccessMode accessMode) {
        this.accessMode = accessMode;
    }

    public String getLatticeUser() {
        return latticeUser;
    }

    public void setLatticeUser(String latticeUser) {
        this.latticeUser = latticeUser;
    }

    public String getExternalAccountId() {
        return externalAccountId;
    }

    public void setExternalAccountId(String externalAccountId) {
        this.externalAccountId = externalAccountId;
    }

}
