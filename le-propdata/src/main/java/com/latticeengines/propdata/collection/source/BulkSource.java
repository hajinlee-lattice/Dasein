package com.latticeengines.propdata.collection.source;

public interface BulkSource extends RawSource {

    String getBulkStageTableName();

    StageServer getBulkStageServer();
}
