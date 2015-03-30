package com.latticeengines.domain.exposed.pls;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.security.EntityAccessRightsData;

public class UserUpdateData {
    private String oldPassword;
    private String newPassword;
    private Map<String, EntityAccessRightsData> rights;
    private String accessLevel;

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

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
