package com.latticeengines.domain.exposed.ratings.coverage;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RatingBucketCoverage {

    @JsonProperty("bucket")
    private String bucket;

    @JsonProperty("count")
    private Long count;

    @JsonProperty("contactCount")
    private Long contactCount;

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Long getContactCount() {
        return contactCount;
    }

    public void setContactCount(Long contactCount) {
        this.contactCount = contactCount;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
