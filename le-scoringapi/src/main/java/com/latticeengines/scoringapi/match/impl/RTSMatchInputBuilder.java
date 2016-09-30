package com.latticeengines.scoringapi.match.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.util.MatchTypeUtil;

@Component
public class RTSMatchInputBuilder extends AbstractMatchInputBuilder {
    @Override
    public boolean accept(String version) {
        return MatchTypeUtil.isValidForRTSBasedMatch(version);
    }
}
