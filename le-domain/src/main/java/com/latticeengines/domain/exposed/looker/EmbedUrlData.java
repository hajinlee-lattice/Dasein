package com.latticeengines.domain.exposed.looker;

import java.util.List;
import java.util.Map;

public class EmbedUrlData {

    private String host;

    private String secret;

    private String externalUserId;

    private String firstName;

    private String lastName;

    private List<Integer> groupIds;

    private List<String> permissions;

    private List<String> models;

    private Map<String, Object> userAttributes;

    private Long sessionLength;

    private String embedUrl;

    private boolean forceLogoutLogin;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getSecret() {
        return secret;
    }

    public void setSecret(String secret) {
        this.secret = secret;
    }

    public String getExternalUserId() {
        return externalUserId;
    }

    public void setExternalUserId(String externalUserId) {
        this.externalUserId = externalUserId;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public List<Integer> getGroupIds() {
        return groupIds;
    }

    public void setGroupIds(List<Integer> groupIds) {
        this.groupIds = groupIds;
    }

    public List<String> getPermissions() {
        return permissions;
    }

    public void setPermissions(List<String> permissions) {
        this.permissions = permissions;
    }

    public List<String> getModels() {
        return models;
    }

    public void setModels(List<String> models) {
        this.models = models;
    }

    public Map<String, Object> getUserAttributes() {
        return userAttributes;
    }

    public void setUserAttributes(Map<String, Object> userAttributes) {
        this.userAttributes = userAttributes;
    }

    public Long getSessionLength() {
        return sessionLength;
    }

    public void setSessionLength(Long sessionLength) {
        this.sessionLength = sessionLength;
    }

    public String getEmbedUrl() {
        return embedUrl;
    }

    public void setEmbedUrl(String embedUrl) {
        this.embedUrl = embedUrl;
    }

    public boolean isForceLogoutLogin() {
        return forceLogoutLogin;
    }

    public void setForceLogoutLogin(boolean forceLogoutLogin) {
        this.forceLogoutLogin = forceLogoutLogin;
    }

    @Override
    public String toString() {
        return "EmbedUrlData{" + "host='" + host + '\'' + ", secret='" + secret + '\'' + ", externalUserId='"
                + externalUserId + '\'' + ", firstName='" + firstName + '\'' + ", lastName='" + lastName + '\''
                + ", groupIds=" + groupIds + ", permissions=" + permissions + ", models=" + models + ", userAttributes="
                + userAttributes + ", sessionLength=" + sessionLength + ", embedUrl='" + embedUrl + '\''
                + ", forceLogoutLogin=" + forceLogoutLogin + '}';
    }
}
