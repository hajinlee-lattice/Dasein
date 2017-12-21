package com.latticeengines.domain.exposed.pls;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.security.EntityAccessRightsData;
import com.latticeengines.domain.exposed.security.Ticket;

public class UserDocument {

    private List<String> errors;
    private UserResult result;
    private boolean success;
    private Ticket ticket;
    private String authenticationRoute;
    
    public UserDocument() {

    }

    @JsonProperty("Errors")
    public List<String> getErrors() {
        return errors;
    }

    @JsonProperty("Errors")
    public void setErrors(List<String> errors) {
        this.errors = errors;
    }

    @JsonProperty("Result")
    public UserResult getResult() {
        return result;
    }

    @JsonProperty("Result")
    public void setResult(UserResult result) {
        this.result = result;
    }

    @JsonProperty("Success")
    public boolean isSuccess() {
        return success;
    }

    @JsonProperty("Success")
    public void setSuccess(boolean success) {
        this.success = success;
    }

    @JsonProperty("Ticket")
    public Ticket getTicket() {
        return ticket;
    }

    @JsonProperty("Ticket")
    public void setTicket(Ticket ticket) {
        this.ticket = ticket;
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

    public class UserResult {

        private User user;

        public UserResult() {

        }

        @JsonProperty("User")
        public User getUser() {
            return user;
        }

        @JsonProperty("User")
        public void setUser(User user) {
            this.user = user;
        }

        public class User {
            private String displayName;
            private String identifier;
            private String emailAddress;
            private String locale;
            private String title;
            private Map<String, EntityAccessRightsData> availableRights;
            private String accessLevel;

            public User() {

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

            @JsonProperty("AvailableRights")
            public Map<String, EntityAccessRightsData> getAvailableRights() {
                return availableRights;
            }

            @JsonProperty("AvailableRights")
            public void setAvailableRights(Map<String, EntityAccessRightsData> availableRights) {
                this.availableRights = availableRights;
            }
        }

    }

}
