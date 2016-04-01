package com.latticeengines.propdata.core.source;

import com.latticeengines.domain.exposed.propdata.StageServer;

public interface BulkSource extends RawSource {

    String getBulkStageTableName();

    StageServer getBulkStageServer();
}
