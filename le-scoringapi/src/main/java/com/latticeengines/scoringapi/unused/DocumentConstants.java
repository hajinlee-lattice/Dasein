package com.latticeengines.scoringapi.unused;

public class DocumentConstants {
    // Metadata
    public static final String SERVICE_NAME = "Skald";
    public static final int DATA_VERSION = 1;

    // Interface names
    public static final String MODEL_INTERFACE = "ModelArtifact";

    // Documents
    public static final String COMBINATION = "/Combinations/%s.json";
    public static final String MODEL_TAGS = "/ModelTags.json";
    public static final String SCORE_DERIVATION_OVERRIDE = "/Overrides/%s/%d/ScoreDerivation.json";

    public static final String DATA_COMPOSITION = "/Models/%s/%d/DataComposition.json";
    public static final String SCORE_DERIVATION = "/Models/%s/%d/ScoreDerivation.json";
}
