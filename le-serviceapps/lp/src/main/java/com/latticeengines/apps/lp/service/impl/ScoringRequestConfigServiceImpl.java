package com.latticeengines.apps.lp.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.lp.entitymgr.ScoringRequestConfigEntityManager;
import com.latticeengines.apps.lp.service.ScoringRequestConfigService;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigContext;

/**
 * @author jadusumalli
 * Serves the ScoringRequestConfiguration functionality for Marketo Integration
 *
 */
@Component
public class ScoringRequestConfigServiceImpl implements ScoringRequestConfigService {

    @Inject
    private ScoringRequestConfigEntityManager scoringRequestConfigEntityMgr;

    @Override
    public ScoringRequestConfigContext retrieveScoringRequestConfigContext(String configUuid) {
        return scoringRequestConfigEntityMgr.retrieveScoringRequestConfigContext(configUuid);
    }

}
