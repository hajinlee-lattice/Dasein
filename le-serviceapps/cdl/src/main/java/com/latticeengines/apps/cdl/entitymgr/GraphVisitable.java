package com.latticeengines.apps.cdl.entitymgr;

public interface GraphVisitable {

    void accept(GraphVisitor visitor, Object entity) throws Exception;

}
