package org.apache.hadoop.yarn.api.records.impl.pb;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;

import com.google.common.base.Preconditions;

public class TestApplicationId extends ApplicationId {
    ApplicationIdProto proto = null;
    ApplicationIdProto.Builder builder = null;

    public TestApplicationId() {
        builder = ApplicationIdProto.newBuilder();
    }

    public TestApplicationId(ApplicationIdProto proto) {
        this.proto = proto;
    }

    public ApplicationIdProto getProto() {
        return proto;
    }

    @Override
    public int getId() {
        Preconditions.checkNotNull(proto);
        return proto.getId();
    }

    @Override
    public void setId(int id) {
        Preconditions.checkNotNull(builder);
        builder.setId(id);
    }

    @Override
    public long getClusterTimestamp() {
        Preconditions.checkNotNull(proto);
        return proto.getClusterTimestamp();
    }

    @Override
    public void setClusterTimestamp(long clusterTimestamp) {
        Preconditions.checkNotNull(builder);
        builder.setClusterTimestamp((clusterTimestamp));
    }

    @Override
    public void build() {
        proto = builder.build();
        builder = null;
    }
}
