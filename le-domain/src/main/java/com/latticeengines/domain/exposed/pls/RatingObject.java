package com.latticeengines.domain.exposed.pls;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RatingObject implements Serializable{


    private static final long serialVersionUID = -2370263562602858887L;

    @JsonProperty("ratingName")
    private String ratingName = "North American Premium Accounts";

    @JsonProperty("description")
    private String description = "";

    @JsonProperty("bucket")
    private String bucket = "A";

    @JsonProperty("count")
    private int count = 100;

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

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

}
