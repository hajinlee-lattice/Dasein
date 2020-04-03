package com.latticeengines.domain.exposed.security;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.auth.GlobalAuthExternalSession;

public class Session {

    @JsonProperty("Tenant")
    private Tenant tenant;

    @JsonProperty("Rights")
    private List<String> rights;

    @JsonProperty("TeamIds")
    private List<String> teamIds;

    @JsonProperty("Ticket")
    private Ticket ticket;

    @JsonProperty("DisplayName")
    private String displayName;

    @JsonProperty("Identifier")
    private String identifier;

    @JsonProperty("EmailAddress")
    private String emailAddress;

    @JsonProperty("Locale")
    private String locale;

    @JsonProperty("Title")
    private String title;

    @JsonProperty("AccessLevel")
    private String accessLevel;

    @JsonProperty("AuthenticationRoute")
    private String authenticationRoute;

    @JsonProperty("ticketCreationTime")
    private Long ticketCreationTime;

    @JsonProperty("ExternalSession")
    private GlobalAuthExternalSession externalSession;

    public Tenant getTenant() {
        return tenant;
    }

    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public List<String> getRights() {
        return rights;
    }

    public void setRights(List<String> rights) {
        this.rights = rights;
    }

    public Ticket getTicket() {
        return ticket;
    }

    public void setTicket(Ticket ticket) {
        this.ticket = ticket;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public String getEmailAddress() {
        return emailAddress;
    }

    public void setEmailAddress(String emailAddress) {
        this.emailAddress = emailAddress;
    }

    public String getLocale() {
        return locale;
    }

    public void setLocale(String locale) {
        this.locale = locale;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getAccessLevel() {
        return accessLevel;
    }

    public void setAccessLevel(String accessLevel) {
        this.accessLevel = accessLevel;
    }

    public String getAuthenticationRoute() {
        return authenticationRoute;
    }

    public void setAuthenticationRoute(String authenticationRoute) {
        this.authenticationRoute = authenticationRoute;
    }

    public GlobalAuthExternalSession getExternalSession() {
        return externalSession;
    }

    public void setExternalSession(GlobalAuthExternalSession externalSession) {
        this.externalSession = externalSession;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public Long getTicketCreationTime() {
        return ticketCreationTime;
    }

    public void setTicketCreationTime(Long ticketCreationTime) {
        this.ticketCreationTime = ticketCreationTime;
    }

    public List<String> getTeamIds() {
        return teamIds;
    }

    public void setTeamIds(List<String> teamIds) {
        this.teamIds = teamIds;
    }
}
