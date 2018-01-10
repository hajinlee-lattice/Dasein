package com.latticeengines.scoringapi.exposed;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;

public class ScoreUtils {

    public static boolean canEnrichInternalAttributes(BatonService batonService, CustomerSpace customerSpace) {
        return batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES);
    }

}
