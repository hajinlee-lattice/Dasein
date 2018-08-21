package com.latticeengines.apps.cdl.entitymgr;

public interface GraphVisitable {
    public void accept(GraphVisitor visitor, Object entity) throws Exception;
}
