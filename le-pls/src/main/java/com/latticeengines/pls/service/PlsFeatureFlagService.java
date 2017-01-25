package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.transform.TransformationGroup;

public interface PlsFeatureFlagService {

    TransformationGroup getTransformationGroupFromZK();

    boolean useDnBFlagFromZK();

    boolean isFuzzyMatchEnabled();

    boolean isV2ProfilingEnabled();

}
