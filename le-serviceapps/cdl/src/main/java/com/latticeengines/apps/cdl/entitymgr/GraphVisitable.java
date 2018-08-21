package com.latticeengines.apps.cdl.entitymgr;

public interface GraphVisitable {
    public void accept(GraphVisitor visitor, String entityId) throws Exception;
}
