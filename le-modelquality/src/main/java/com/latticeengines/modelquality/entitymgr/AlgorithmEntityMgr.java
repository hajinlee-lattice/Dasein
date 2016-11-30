package com.latticeengines.modelquality.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.modelquality.Algorithm;

public interface AlgorithmEntityMgr extends BaseEntityMgr<Algorithm> {

    Algorithm findByName(String algorithmName);
    
    Algorithm getLatestProductionVersion();
}
