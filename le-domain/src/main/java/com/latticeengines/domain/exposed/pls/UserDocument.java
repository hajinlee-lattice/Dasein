package com.latticeengines.domain.exposed.pls;

import java.util.ArrayList;
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

        @JsonProperty("User")
        private User user;

        public UserResult() {

        }

        public User getUser() {
            return user;
        }

        public void setUser(User user) {
            this.user = user;
        }

        public class User {
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
            @JsonProperty("AvailableRights")
            private Map<String, EntityAccessRightsData> availableRights;
            @JsonProperty("AccessLevel")
            private String accessLevel;
            @JsonProperty("TeamIds")
            private List<String> teamIds = new ArrayList<>();

            public User() {

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

            public Map<String, EntityAccessRightsData> getAvailableRights() {
                return availableRights;
            }

            public void setAvailableRights(Map<String, EntityAccessRightsData> availableRights) {
                this.availableRights = availableRights;
            }

            public List<String> getTeamIds() {
                return teamIds;
            }

            public void setTeamIds(List<String> teamIds) {
                this.teamIds = teamIds;
            }
        }

    }

}
