package com.latticeengines.datacloud.match.service.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.exposed.service.RealTimeMatchService;
import com.latticeengines.domain.exposed.util.MatchTypeUtil;

@Component("realTimeMatchWithDerivedColumnCacheService")
public class RealTimeMatchWithDerivedColumnCacheServiceImpl extends RealTimeMatchServiceBase implements RealTimeMatchService {

    @Override
    public boolean accept(String version) {
        return MatchTypeUtil.isValidForRTSBasedMatch(version);
    }

}
