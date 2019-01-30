package com.latticeengines.domain.exposed.scoring;

import org.apache.avro.Schema.Type;

import com.latticeengines.domain.exposed.metadata.InterfaceName;

public enum ScoreResultField {

    Percentile(Type.INT.name(), "Score", Integer.class.getSimpleName()), //
    RawScore(Type.DOUBLE.name(), "RawScore", Double.class.getSimpleName()), //
    Rating(Type.STRING.name(), InterfaceName.Rating.name(), String.class.getSimpleName()), //
    ModelId(Type.STRING.name(), "Model_GUID", String.class.getSimpleName()), //
    InternalId(Type.STRING.name(), InterfaceName.InternalId.name(), Long.class.getSimpleName()), //
    Probability(Type.DOUBLE.name(), InterfaceName.Probability.name(), Double.class.getSimpleName()), //
    NormalizedScore(Type.DOUBLE.name(), InterfaceName.NormalizedScore.name(),
            Double.class.getSimpleName()), //
    PredictedRevenue(Type.DOUBLE.name(), InterfaceName.PredictedRevenue.name(),
            Double.class.getSimpleName()), //
    ExpectedRevenue(Type.DOUBLE.name(), InterfaceName.ExpectedRevenue.name(),
            Double.class.getSimpleName()), PredictedRevenuePercentile(Type.INT.name(),
                    "PredictedRevenuePercentile", Integer.class.getSimpleName()), //
    ExpectedRevenuePercentile(Type.INT.name(), "ExpectedRevenuePercentile",
            Integer.class.getSimpleName()); //

    public String physicalDataType;

    public String displayName;

    public String sourceLogicalDataType;

    ScoreResultField(String physicalDataType, String displayName, String sourceLogicalDataType) {
        this.physicalDataType = physicalDataType;
        this.sourceLogicalDataType = sourceLogicalDataType;
        this.displayName = displayName;
    }

}
