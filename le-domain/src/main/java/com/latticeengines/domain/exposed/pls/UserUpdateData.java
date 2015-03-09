package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class UserUpdateData {
    private String oldPassword;
    private String newPassword;

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

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
