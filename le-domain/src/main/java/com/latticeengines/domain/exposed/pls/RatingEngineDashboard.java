package com.latticeengines.domain.exposed.pls;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;

public class RatingEngineDashboard {

    private CoverageInfo coverageInfo;

    private RatingEngineSummary summary;

    private List<Play> plays;

    private MetadataSegment segment;

    public CoverageInfo getCoverageInfo() {
        return coverageInfo;
    }

    public RatingEngineSummary getSummary() {
        return summary;
    }

    public List<Play> getPlays() {
        return plays;
    }

    public MetadataSegment getSegment() {
        return segment;
    }

    public void setCoverageInfo(CoverageInfo coverageInfo) {
        this.coverageInfo = coverageInfo;
    }

    public void setSummary(RatingEngineSummary summary) {
        this.summary = summary;
    }

    public void setPlays(List<Play> plays) {
        this.plays = plays;
    }

    public void setSegment(MetadataSegment segment) {
        this.segment = segment;
    }
}
