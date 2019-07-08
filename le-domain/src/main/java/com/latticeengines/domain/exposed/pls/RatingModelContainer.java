package com.latticeengines.domain.exposed.pls;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class RatingModelContainer {

    @JsonProperty("model")
    private RatingModel model;

    @JsonProperty("engine")
    private RatingEngineSummary engineSummary;

    @JsonProperty("scoringBucketMetadata")
    private List<BucketMetadata> scoringBucketMetadata;

    @JsonProperty("extractedTarget")
    private HdfsDataUnit extractedTarget;

    // for jackson
    @SuppressWarnings("unused")
    private RatingModelContainer() {
    }

    public RatingModelContainer(RatingModel ratingModel) {
        this.model = ratingModel;
    }

    public RatingModelContainer(RatingModel ratingModel, RatingEngineSummary engineSummary,
            List<BucketMetadata> scoringBuckets) {
        this.model = ratingModel;
        this.engineSummary = engineSummary;
        this.scoringBucketMetadata = scoringBuckets;
    }

    public RatingModel getModel() {
        return model;
    }

    public void setModel(RatingModel model) {
        this.model = model;
    }

    public RatingEngineSummary getEngineSummary() {
        return engineSummary;
    }

    public void setEngineSummary(RatingEngineSummary engineSummary) {
        this.engineSummary = engineSummary;
    }

    public List<BucketMetadata> getScoringBucketMetadata() {
        return scoringBucketMetadata;
    }

    public void setScoringBucketMetadata(List<BucketMetadata> scoringBucketMetadata) {
        this.scoringBucketMetadata = scoringBucketMetadata;
    }

    public HdfsDataUnit getExtractedTarget() {
        return extractedTarget;
    }

    public void setExtractedTarget(HdfsDataUnit extractedTarget) {
        this.extractedTarget = extractedTarget;
    }
}
