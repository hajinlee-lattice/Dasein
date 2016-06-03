package com.latticeengines.scoringapi.score.impl;

import java.util.Map;
import java.util.UUID;

public class LatticeIdGenerator {

    public static String generateLatticeId(Map<String, Object> attributeValues) {
        // this is only temporary implementation and it will be replaced when
        // lattice id txn is merged
        return UUID.randomUUID().toString();
    }

}
