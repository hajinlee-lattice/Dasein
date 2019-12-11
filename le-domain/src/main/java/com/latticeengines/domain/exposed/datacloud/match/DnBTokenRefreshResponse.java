package com.latticeengines.domain.exposed.datacloud.match;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class DnBTokenRefreshResponse {

    @JsonProperty("KeyType")
    private DnBKeyType keyType;

    @JsonProperty("NewToken")
    private String newToken;

    public DnBTokenRefreshResponse(DnBKeyType keyType, String newToken) {
        this.keyType = keyType;
        this.newToken = newToken;
    }

    // for serialization
    private DnBTokenRefreshResponse() {

    }

    public DnBKeyType getKeyType() {
        return keyType;
    }

    public void setKeyType(DnBKeyType keyType) {
        this.keyType = keyType;
    }

    public String getNewToken() {
        return newToken;
    }

    public void setNewToken(String newToken) {
        this.newToken = newToken;
    }
}
