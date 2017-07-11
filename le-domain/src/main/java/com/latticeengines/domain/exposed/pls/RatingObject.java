package com.latticeengines.domain.exposed.pls;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RatingObject implements Serializable{


    private static final long serialVersionUID = -2370263562602858887L;


    @JsonProperty("bucket")
    private String bucket = "A";

    @JsonProperty("count")
    private int count = 0;


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
