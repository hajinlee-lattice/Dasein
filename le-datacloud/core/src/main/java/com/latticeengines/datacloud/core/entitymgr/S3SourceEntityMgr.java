package com.latticeengines.datacloud.core.entitymgr;

public interface S3SourceEntityMgr {

    void downloadToHdfs(String sourceName, String version, String distCpQueue);

}
