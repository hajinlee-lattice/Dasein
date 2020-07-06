package com.latticeengines.apps.lp.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfig;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigContext;

public interface ScoringRequestConfigEntityManager extends BaseEntityMgrRepository<ScoringRequestConfig, Long> {

    ScoringRequestConfigContext retrieveScoringRequestConfigContext(String configUuid);
}
