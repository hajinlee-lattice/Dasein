package com.latticeengines.domain.exposed.pls;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.cdl.CDLObjectTypes;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.ratings.coverage.CoverageInfo;

public class RatingEngineDashboard {

    private CoverageInfo coverageInfo;

    private RatingEngineSummary summary;

    private List<Play> plays;

    private MetadataSegment segment;

    private Map<CDLObjectTypes, List<String>> dependencies;

    private List<RatingModelDTO> iterations;

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

    public Map<CDLObjectTypes, List<String>> getDependencies() {
        return dependencies;
    }

    public void setDependencies(Map<CDLObjectTypes, List<String>> dependencies) {
        this.dependencies = dependencies;
    }

    public List<RatingModelDTO> getIterations() {
        return iterations;
    }

    public void setIterations(List<RatingModelDTO> iterations) {
        this.iterations = iterations;
    }
}
