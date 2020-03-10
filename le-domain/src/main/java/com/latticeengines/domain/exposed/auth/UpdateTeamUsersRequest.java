package com.latticeengines.domain.exposed.auth;

import java.util.Set;

public class UpdateTeamUsersRequest {

    private Set<String> userToAssign;

    public Set<String> getUserToAssign() {
        return userToAssign;
    }

    public void setUserToAssign(Set<String> userToAssign) {
        this.userToAssign = userToAssign;
    }
}
