package com.latticeengines.apps.lp.service.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.lp.entitymgr.ScoringRequestConfigEntityManager;
import com.latticeengines.apps.lp.service.ScoringRequestConfigService;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
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
        if (StringUtils.isBlank(configUuid)) {
            throw new LedpException(LedpCode.LEDP_18194, new String[] {configUuid});
        }
        return scoringRequestConfigEntityMgr.retrieveScoringRequestConfigContext(configUuid);
    }

}
