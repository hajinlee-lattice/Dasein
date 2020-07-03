package com.latticeengines.domain.exposed.auth;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TeamEntities {
    @JsonProperty("MetadataSegments")
    private List<MetadataSegment> metadataSegments;

    @JsonProperty("Plays")
    private List<Play> plays;

    @JsonProperty("RatingEngines")
    private List<RatingEngineSummary> ratingEngineSummaries;

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
}
