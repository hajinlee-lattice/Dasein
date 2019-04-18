package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfig;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigContext;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigSummary;

public interface ScoringRequestConfigEntityManager extends BaseEntityMgrRepository<ScoringRequestConfig, Long> {

    List<ScoringRequestConfigSummary> findAllByMarketoCredential(Long credentialPid);

    ScoringRequestConfig findByModelUuid(Long credentialPid, String modelUuid);

    ScoringRequestConfig findByConfigId(Long credentialPid, String configId);

    ScoringRequestConfigContext retrieveScoringRequestConfigContext(String configUuid);
}
