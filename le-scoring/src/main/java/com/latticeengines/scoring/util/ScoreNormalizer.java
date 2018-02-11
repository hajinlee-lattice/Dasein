package com.latticeengines.scoring.util;

public interface ScoreNormalizer {

    double normalize(double score, InterpolationFunctionType interpFunction);

    boolean isInitialized();
}
