package com.latticeengines.domain.exposed.datacloud.statistics;

import java.util.List;

public class Buckets {
    private BucketType type;

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
