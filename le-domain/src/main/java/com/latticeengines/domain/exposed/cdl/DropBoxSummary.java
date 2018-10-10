package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class DropBoxSummary {

    @JsonProperty("Bucket")
    private String bucket;

    @JsonProperty("DropBox")
    private String dropBox;

    @JsonProperty("AccessMode")
    private DropBoxAccessMode accessMode;

    @JsonProperty("ExternalAccount")
    private String externalAccount;

    @JsonProperty("LatticeUser")
    private String latticeUser;

    @JsonProperty("AccessKeyId")
    private String accessKeyId;

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getDropBox() {
        return dropBox;
    }

    public void setDropBox(String dropBox) {
        this.dropBox = dropBox;
    }

    public DropBoxAccessMode getAccessMode() {
        return accessMode;
    }

    public void setAccessMode(DropBoxAccessMode accessMode) {
        this.accessMode = accessMode;
    }

    public String getExternalAccount() {
        return externalAccount;
    }

    public void setExternalAccount(String externalAccount) {
        this.externalAccount = externalAccount;
    }

    public String getLatticeUser() {
        return latticeUser;
    }

    public void setLatticeUser(String latticeUser) {
        this.latticeUser = latticeUser;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public void setAccessKeyId(String accessKeyId) {
        this.accessKeyId = accessKeyId;
    }
}
