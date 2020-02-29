package com.latticeengines.security.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class IDaaSUser {

    @JsonProperty("user_name")
    private String userName;

    @JsonProperty("email_address")
    private String emailAddress;

    @JsonProperty("first_name")
    private String firstName;

    @JsonProperty("last_name")
    private String lastName;

    @JsonProperty("full_name")
    private String fullName;

    @JsonProperty("applications")
    private List<String> applications;

    @JsonProperty("roles")
    private List<String> roles;

    @JsonProperty("apiUserStatus")
    private String apiUserStatus;

    @JsonProperty("webUserStatus")
    private String webUserStatus;

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getEmailAddress() {
        return emailAddress;
    }

    public void setEmailAddress(String emailAddress) {
        this.emailAddress = emailAddress;
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

    public String getFullName() {
        return fullName;
    }

    public void setFullName(String fullName) {
        this.fullName = fullName;
    }

    public List<String> getApplications() {
        return new ArrayList<>(CollectionUtils.emptyIfNull(applications));
    }

    public void setApplications(List<String> applications) {
        this.applications = applications;
    }

    public List<String> getRoles() {
        return new ArrayList<>(CollectionUtils.emptyIfNull(roles));
    }

    public void setRoles(List<String> roles) {
        this.roles = roles;
    }

    public String getApiUserStatus() {
        return apiUserStatus;
    }

    public void setApiUserStatus(String apiUserStatus) {
        this.apiUserStatus = apiUserStatus;
    }

    public String getWebUserStatus() {
        return webUserStatus;
    }

    public void setWebUserStatus(String webUserStatus) {
        this.webUserStatus = webUserStatus;
    }
}
