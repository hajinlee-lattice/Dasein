package com.latticeengines.domain.exposed.auth;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.security.User;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GlobalTeam {

    @JsonProperty("TeamId")
    private String teamId;

    @JsonProperty("TeamName")
    private String teamName;

    @JsonProperty("CreatedBy")
    private User createdByUser;

    @JsonProperty("created")
    private Date created;

    // this is different with last update time, last used time means
    // latest update time on entity that associated with team.
    @JsonProperty("lastUsed")
    private Date lastUsed;

    @JsonProperty("TeamMembers")
    private List<User> teamMembers;

    @JsonProperty("MetadataSegments")
    private List<MetadataSegment> metadataSegments;

    @JsonProperty("Plays")
    private List<Play> plays;

    @JsonProperty("RatingEngines")
    private List<RatingEngineSummary> ratingEngineSummaries;

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

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    public List<MetadataSegment> getMetadataSegments() {
        return metadataSegments;
    }

    public void setMetadataSegments(List<MetadataSegment> metadataSegments) {
        this.metadataSegments = metadataSegments;
    }

    public List<Play> getPlays() {
        return plays;
    }

    public void setPlays(List<Play> plays) {
        this.plays = plays;
    }

    public List<RatingEngineSummary> getRatingEngineSummaries() {
        return ratingEngineSummaries;
    }

    public void setRatingEngineSummaries(List<RatingEngineSummary> ratingEngineSummaries) {
        this.ratingEngineSummaries = ratingEngineSummaries;
    }

    public Date getLastUsed() {
        return lastUsed;
    }

    public void setLastUsed(Date lastUsed) {
        this.lastUsed = lastUsed;
    }
}
