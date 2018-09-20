package com.latticeengines.domain.exposed.pls;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RatingModelWithPublishedHistoryDTO extends RatingModelDTO {

    public RatingModelWithPublishedHistoryDTO() {
    }

    public RatingModelWithPublishedHistoryDTO(AIModel ratingModel, Long publishedTimestamp,
            List<BucketMetadata> publishedMetadata, String publishedBy) {
        super(ratingModel);
        this.publishedTimestamp = publishedTimestamp;
        this.publishedMetadata = publishedMetadata;
        this.publishedBy = publishedBy;
    }

    @JsonProperty("publishedMetadata")
    private List<BucketMetadata> publishedMetadata;

    @JsonProperty("publishedTimestamp")
    private Long publishedTimestamp;

    @JsonProperty("publishedBy")
    private String publishedBy;

    public List<BucketMetadata> getPublishedMetadata() {
        return publishedMetadata;
    }

    public void setPublishedMetadata(List<BucketMetadata> publishedMetadata) {
        this.publishedMetadata = publishedMetadata;
    }

    public Long getPublishedTimestamp() {
        return publishedTimestamp;
    }

    public void setPublishedTimestamp(Long publishedTimestamp) {
        this.publishedTimestamp = publishedTimestamp;
    }

    public String getPublishedBy() {
        return publishedBy;
    }

    public void setPublishedBy(String publishedBy) {
        this.publishedBy = publishedBy;
    }
}
