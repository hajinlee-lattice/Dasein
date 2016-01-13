package com.latticeengines.propdata.core.source;

public interface BulkSource extends RawSource {

    String getBulkStageTableName();

    StageServer getBulkStageServer();
}
