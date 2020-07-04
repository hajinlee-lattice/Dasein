package com.latticeengines.apps.lp.service;

import com.latticeengines.domain.exposed.pls.ScoringRequestConfigContext;

public interface ScoringRequestConfigService {

    ScoringRequestConfigContext retrieveScoringRequestConfigContext(String configUuid);
}
