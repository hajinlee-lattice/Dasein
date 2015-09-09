package com.latticeengines.domain.exposed.camille.featureflags;

import java.util.HashMap;

public class FeatureFlagValueMap extends HashMap<String, Boolean> {
    private static final long serialVersionUID = 1L;

    public FeatureFlagValueMap() { super(); }
    public FeatureFlagValueMap(FeatureFlagValueMap map) { super(map); }

}
