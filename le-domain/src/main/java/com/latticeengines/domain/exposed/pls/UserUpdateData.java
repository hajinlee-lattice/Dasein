package com.latticeengines.domain.exposed.pls;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.security.EntityAccessRightsData;

public class UserUpdateData {
    private String oldPassword;
    private String newPassword;
    private Map<String, EntityAccessRightsData> rights;
    private String accessLevel;
    private Long expirationDate;
    private List<String> userTeams;

    @JsonProperty("OldPassword")
    public String getOldPassword() {
        return oldPassword;
    }

    @JsonProperty("OldPassword")
    public void setOldPassword(String oldPassword) {
        this.oldPassword = oldPassword;
    }

    @JsonProperty("NewPassword")
    public String getNewPassword() {
        return newPassword;
    }

    @JsonProperty("NewPassword")
    public void setNewPassword(String newPassword) {
        this.newPassword = newPassword;
    }

    @JsonProperty("Rights")
    public Map<String, EntityAccessRightsData> getRights() {
        return rights;
    }

    @JsonProperty("Rights")
    public void setRights(Map<String, EntityAccessRightsData> rights) {
        this.rights = rights;
    }

    @JsonProperty("AccessLevel")
    public String getAccessLevel() {
        return accessLevel;
    }

    @JsonProperty("AccessLevel")
    public void setAccessLevel(String accessLevel) {
        this.accessLevel = accessLevel;
    }

    @JsonProperty("ExpirationDate")
    public Long getExpirationDate() {
        return expirationDate;
    }

    @JsonProperty("ExpirationDate")
    public void setExpirationDate(Long expirationDate) {
        this.expirationDate = expirationDate;
    }

    @Override
    public String toString() {
        // make sure that when this object is logged, we hide user password
        // (even though it is encrypted). First clone this object and remove
        // password from that cloned obj
        UserUpdateData userUpdateDataForLogging = JsonUtils.deserialize(JsonUtils.serialize(this),
                UserUpdateData.class);
        userUpdateDataForLogging.setNewPassword("<<password_hidden>>");
        userUpdateDataForLogging.setOldPassword("<<password_hidden>>");
        return JsonUtils.serialize(userUpdateDataForLogging);
    }

    @JsonProperty("UserTeams")
    public List<String> getUserTeams() {
        return userTeams;
    }

    @JsonProperty("UserTeams")
    public void setUserTeams(List<String> userTeams) {
        this.userTeams = userTeams;
    }
}
