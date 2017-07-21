package com.latticeengines.domain.exposed.pls;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RatingObject implements Serializable {

    private static final long serialVersionUID = -2370263562602858887L;

    @JsonProperty("ratingName")
    private String ratingName = "North American Premium Accounts";

    @JsonProperty("description")
    private String description = "North American Premium Accounts";

    @JsonProperty("bucketInformation")
    private List<BucketInformation> bucketInfoList;

    public String getRatingName() {
        return ratingName;
    }

    public void setRatingName(String ratingName) {
        this.ratingName = ratingName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<BucketInformation> getBucketInfoList() {
        return this.bucketInfoList;
    }

    public void setBucketInfoList(List<BucketInformation> bucketInfoList) {
        this.bucketInfoList = bucketInfoList;
    }

}
