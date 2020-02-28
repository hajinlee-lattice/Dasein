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

    public Set<String> getUserToRemove() {
        return userToRemove;
    }

    public void setUserToRemove(Set<String> userToRemove) {
        this.userToRemove = userToRemove;
    }
}
