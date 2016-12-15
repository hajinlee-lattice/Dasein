package com.latticeengines.domain.exposed.datafabric;

import com.latticeengines.domain.exposed.dataplatform.HasId;

public class TimeSeriesFabricEntity extends CompositeFabricEntity implements HasId<String> {

    @DynamoBucketKey()
    String bucket;

    @DynamoStampKey()
    String stamp;

    public void setId(String parentId, String entityName, String entityId,
                      String bucket, String stamp) {
        super.setId(parentId, entityName, entityId);
        this.bucket = bucket;
        this.stamp = stamp;
    }

    @Override
    public String getId() {
        return super.getId() + "#" + bucket + "#" + stamp;
    }

    @Override
    public void setId(String id) {
        String[] ids = id.split("#");
        if (ids.length != 5) {
            return;
        } else {
            super.setId(ids[0], ids[1], ids[2]);
            bucket = ids[3];
            stamp = ids[4];
        }
    }

    public String getBucket() {
        return bucket;
    }

    public String getStamp() {
        return stamp;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public void setStamp(String stamp) {
        this.stamp = stamp;
    }
}
