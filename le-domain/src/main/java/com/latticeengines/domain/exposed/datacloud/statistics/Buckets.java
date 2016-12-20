package com.latticeengines.domain.exposed.datacloud.statistics;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Buckets {
    @JsonProperty("Type")
    private BucketType type;

    @JsonProperty("List")
    private List<Bucket> bucketList;

    public BucketType getType() {
        return type;
    }

    public void setType(BucketType type) {
        this.type = type;
    }

    public List<Bucket> getBucketList() {
        return bucketList;
    }

    public void setBucketList(List<Bucket> bucketList) {
        this.bucketList = bucketList;
    }

}
