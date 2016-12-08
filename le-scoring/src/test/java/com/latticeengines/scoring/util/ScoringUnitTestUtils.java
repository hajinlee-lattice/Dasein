package com.latticeengines.scoring.util;

import java.util.UUID;

public class ScoringUnitTestUtils {

    public static String generateRandomModelId() {
        return String.format("ms__%s-PLSModel", UUID.randomUUID());
    }

}
