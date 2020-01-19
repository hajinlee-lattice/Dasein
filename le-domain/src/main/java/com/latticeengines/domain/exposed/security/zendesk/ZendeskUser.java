package com.latticeengines.domain.exposed.security.zendesk;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ZendeskUser {
    private Long id;
    private String name;
    private String email;
    private String externalId;
    private Boolean active;
    private Boolean verified;
    private Boolean suspended;
    private ZendeskRole role;

    public ZendeskUser() {
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    @JsonProperty("external_id")
    public String getExternalId() {
        return externalId;
    }

    @JsonProperty("external_id")
    public void setExternalId(String externalId) {
        this.externalId = externalId;
    }

    public Boolean getActive() {
        return active;
    }

    public void setActive(Boolean active) {
        this.active = active;
    }

    public Boolean getVerified() {
        return verified;
    }

    public void setVerified(Boolean verified) {
        this.verified = verified;
    }

    public Boolean getSuspended() {
        return suspended;
    }

    public void setSuspended(Boolean suspended) {
        this.suspended = suspended;
    }

    @JsonIgnore
    public ZendeskRole getRole() {
        return role;
    }

    @JsonIgnore
    public void setRole(ZendeskRole role) {
        this.role = role;
    }

    @JsonProperty("role")
    public String getRoleName() {
        return role == null ? null : role.toString();
    }

    @JsonProperty("role")
    public void setRole(String role) {
        try {
            this.role = ZendeskRole.fromString(role);
        } catch (Exception ignore) {
            // ignore parsing error
        }
    }
}
