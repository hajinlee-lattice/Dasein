package com.latticeengines.scoringapi.exposed;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;

public final class ScoreUtils {

    protected ScoreUtils() {
        throw new UnsupportedOperationException();
    }

    public static boolean canEnrichInternalAttributes(BatonService batonService, CustomerSpace customerSpace) {
        return batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES);
    }

}
