package com.latticeengines.metadata.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.VdbImportExtract;

public interface VdbImportExtractEntityMgr extends BaseEntityMgr<VdbImportExtract> {

    VdbImportExtract findByExtractIdentifier(String identifier);
}
