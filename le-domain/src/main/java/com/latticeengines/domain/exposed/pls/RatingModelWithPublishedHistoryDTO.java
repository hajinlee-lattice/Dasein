package com.latticeengines.domain.exposed.pls;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RatingModelWithPublishedHistoryDTO extends RatingModelDTO {

    public RatingModelWithPublishedHistoryDTO() {
    }

    public RatingModelWithPublishedHistoryDTO(RatingModel ratingModel,
            Map<Long, List<BucketMetadata>> publishedHistory) {
        super(ratingModel);
        this.publishedHistory = publishedHistory;
    }

    @JsonProperty("publishedHistory")
    private Map<Long, List<BucketMetadata>> publishedHistory;

    public Map<Long, List<BucketMetadata>> getPublishedHistory() {
        return publishedHistory;
    }

    public void setPublishedHistory(Map<Long, List<BucketMetadata>> publishedHistory) {
        this.publishedHistory = publishedHistory;
    }
}
