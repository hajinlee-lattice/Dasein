package com.latticeengines.domain.exposed.pls;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GlobalTeamData {

    @JsonProperty("teamName")
    private String teamName;

    private Set<String> teamMembers;

    public String getTeamName() {
        return teamName;
    }

    public void setTeamName(String teamName) {
        this.teamName = teamName;
    }

    public Set<String> getTeamMembers() {
        return teamMembers;
    }

    public void setTeamMembers(Set<String> teamMembers) {
        this.teamMembers = teamMembers;
    }
}
