package com.latticeengines.domain.exposed.datacloud.statistics;

import java.util.ArrayList;
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

    @JsonProperty("HasMore")
    private Boolean hasMore;

    public Buckets() {
    }

    public Buckets(Buckets buckets) {
        // used for deep copy during stats calculation
        this();
        this.type = buckets.type;
        if (buckets.bucketList != null) {
            this.bucketList = new ArrayList<>();
            for (Bucket oBucket : buckets.bucketList) {
                Bucket nBucket = new Bucket(oBucket);
                this.bucketList.add(nBucket);
            }
        }
    }

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

    public Boolean getHasMore() {
        return hasMore;
    }

    public void setHasMore(Boolean hasMore) {
        this.hasMore = hasMore;
    }
}
