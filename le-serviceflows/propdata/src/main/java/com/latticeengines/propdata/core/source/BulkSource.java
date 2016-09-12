package com.latticeengines.propdata.core.source;

import com.latticeengines.domain.exposed.datacloud.StageServer;

public interface BulkSource extends DataImportedFromDB {

    String getBulkStageTableName();

    StageServer getBulkStageServer();
}
