package com.latticeengines.domain.exposed.auth;

import java.util.Set;

public class UpdateTeamUsersRequest {
    Set<String> userToAssign;
    Set<String> userToRemove;

    public Set<String> getUserToAssign() {
        return userToAssign;
    }

    public void setUserToAssign(Set<String> userToAssign) {
        this.userToAssign = userToAssign;
    }
}
