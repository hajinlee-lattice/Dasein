package com.latticeengines.scoringapi.match.impl;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

@Component
public class RTSMatchInputBuilder extends AbstractMatchInputBuilder {
    @Override
    public boolean accept(String version) {
        return StringUtils.isEmpty(version) || ( version.compareTo("2") < 0 );
    }
}
