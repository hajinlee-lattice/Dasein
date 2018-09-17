package com.latticeengines.domain.exposed.cdl;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class GrantDropBoxAccessResponse {

    @JsonProperty("UseAccessMode")
    private DropBoxAccessMode accessMode;

    @JsonProperty("LatticeUser")
    private String latticeUser;

    @JsonProperty("AccessKey")
    private String accessKey;

    @JsonProperty("SecretKey")
    private String secretKey;

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

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getExternalAccountId() {
        return externalAccountId;
    }

    public void setExternalAccountId(String externalAccountId) {
        this.externalAccountId = externalAccountId;
    }

}
