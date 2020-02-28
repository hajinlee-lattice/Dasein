package com.latticeengines.domain.exposed.auth;

import java.util.Set;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.UuidUtils;

public class GlobalTeam {

    @JsonProperty("TeamId")
    private String teamId;

    @JsonProperty("TeamName")
    private String teamName;

    @JsonProperty("CreatedBy")
    private String createdByUser;

    @JsonProperty("TeamMembers")
    Set<String> teamMembers;

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

    public String getCreatedByUser() {
        return createdByUser;
    }

    public void setCreatedByUser(String createdByUser) {
        this.createdByUser = createdByUser;
    }

    public Set<String> getTeamMembers() {
        return teamMembers;
    }

    public void setTeamMembers(Set<String> teamMembers) {
        this.teamMembers = teamMembers;
    }

    @Override
    public String toString() {
        String team = "TeamId: " + this.teamId + ", TeamName: " + this.teamName + //
                ", Created_By_User: " + this.createdByUser + //
                ", TeamMembers: " + this.teamMembers;
        return team;
    }
}
