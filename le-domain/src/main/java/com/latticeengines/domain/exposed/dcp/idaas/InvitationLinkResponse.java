package com.latticeengines.domain.exposed.dcp.idaas;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class InvitationLinkResponse {

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("source")
    private String source;

    @JsonProperty("status")
    private String status;

    @JsonProperty("inviteLink")
    private String inviteLink;

    @JsonProperty("principal_type")
    private String principalType;

    public String getUserId() {
        return userId;
    }

    public String getSource() {
        return source;
    }

    public String getStatus() {
        return status;
    }

    public String getInviteLink() {
        return inviteLink;
    }

    public String getPrincipalType() {
        return principalType;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void setInviteLink(String inviteLink) {
        this.inviteLink = inviteLink;
    }

    public void setPrincipalType(String principalType) {
        this.principalType = principalType;
    }
}
