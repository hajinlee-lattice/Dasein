package com.latticeengines.domain.exposed.pls;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.security.User;

import java.util.List;

public class DeleteUsersResult {
    private List<String> successUsers;
    private List<String> failUsers;

    @JsonProperty("SuccessUsers")
    public List<String> getSuccessUsers() { return successUsers; }

    @JsonProperty("SuccessUsers")
    public void setSuccessUsers(List<String> successUsers) { this.successUsers = successUsers; }

    @JsonProperty("FailUsers")
    public List<String> getFailUsers() { return failUsers; }

    @JsonProperty("FailUsers")
    public void setFailUsers(List<String> failUsers) { this.failUsers = failUsers; }

    @Override
    public String toString() { return JsonUtils.serialize(this); }
}
