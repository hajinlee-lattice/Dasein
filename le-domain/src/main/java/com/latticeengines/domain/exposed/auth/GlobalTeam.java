package com.latticeengines.domain.exposed.auth;

import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.security.User;

public class GlobalTeam {

    @JsonProperty("TeamId")
    private String teamId;

    @JsonProperty("TeamName")
    private String teamName;

    @JsonProperty("CreatedBy")
    private User createdByUser;

    @JsonProperty("TeamMembers")
    private List<User> teamMembers;

    public static String generateId() {
        return "Team_" + AvroUtils.getAvroFriendlyString(UuidUtils.shortenUuid(UUID.randomUUID()));
    }

    public String getTeamId() {
        return teamId;
    }

    public void setTeamId(String teamId) {
        this.teamId = teamId;
    }

    public String getTeamName() {
        return teamName;
    }

    public void setTeamName(String teamName) {
        this.teamName = teamName;
    }

    public User getCreatedByUser() {
        return createdByUser;
    }

    public void setCreatedByUser(User createdByUser) {
        this.createdByUser = createdByUser;
    }

    @Override
    public String toString() {
        String team = "TeamId: " + this.teamId + ", TeamName: " + this.teamName + //
                ", Created_By_User: " + this.createdByUser + //
                ", TeamMembers: " + this.teamMembers;
        return team;
    }

    public void setTeamMembers(List<User> teamMembers) {
        this.teamMembers = teamMembers;
    }

    public List<User> getTeamMembers() {
        return teamMembers;
    }
}
