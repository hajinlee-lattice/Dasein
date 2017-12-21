package com.latticeengines.domain.exposed.security;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class Session {

    private Tenant tenant;
    private List<String> rights;
    private Ticket ticket;
    private String displayName;
    private String identifier;
    private String emailAddress;
    private String locale;
    private String title;
    private String accessLevel;
    private String authenticationRoute;

    @JsonProperty("Tenant")
    public Tenant getTenant() {
        return tenant;
    }

    @JsonProperty("Tenant")
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    @JsonProperty("Rights")
    public List<String> getRights() {
        return rights;
    }

    @JsonProperty("Rights")
    public void setRights(List<String> rights) {
        this.rights = rights;
    }

    @JsonProperty("Ticket")
    public Ticket getTicket() {
        return ticket;
    }

    @JsonProperty("Ticket")
    public void setTicket(Ticket ticket) {
        this.ticket = ticket;
    }

    @JsonProperty("DisplayName")
    public String getDisplayName() {
        return displayName;
    }

    @JsonProperty("DisplayName")
    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @JsonProperty("Identifier")
    public String getIdentifier() {
        return identifier;
    }

    @JsonProperty("Identifier")
    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    @JsonProperty("EmailAddress")
    public String getEmailAddress() {
        return emailAddress;
    }

    @JsonProperty("EmailAddress")
    public void setEmailAddress(String emailAddress) {
        this.emailAddress = emailAddress;
    }

    @JsonProperty("Locale")
    public String getLocale() {
        return locale;
    }

    @JsonProperty("Locale")
    public void setLocale(String locale) {
        this.locale = locale;
    }

    @JsonProperty("Title")
    public String getTitle() {
        return title;
    }

    @JsonProperty("Title")
    public void setTitle(String title) {
        this.title = title;
    }

    @JsonProperty("AccessLevel")
    public String getAccessLevel() {
        return accessLevel;
    }

    @JsonProperty("AccessLevel")
    public void setAccessLevel(String accessLevel) {
        this.accessLevel = accessLevel;
    }
    
    @JsonProperty("AuthenticationRoute")
    public String getAuthenticationRoute() {
        return authenticationRoute;
    }

    @JsonProperty("AuthenticationRoute")
    public void setAuthenticationRoute(String authenticationRoute) {
        this.authenticationRoute = authenticationRoute;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
