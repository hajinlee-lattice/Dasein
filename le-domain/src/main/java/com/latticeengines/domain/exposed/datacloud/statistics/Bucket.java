package com.latticeengines.domain.exposed.datacloud.statistics;

public class Bucket {
    private String bucketLabel;

    private Integer count;

    public String getBucketLabel() {
        return bucketLabel;
    }

    public void setBucketLabel(String bucketLabel) {
        this.bucketLabel = bucketLabel;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }
}
