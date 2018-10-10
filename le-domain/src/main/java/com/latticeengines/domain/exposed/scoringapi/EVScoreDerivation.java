package com.latticeengines.domain.exposed.scoringapi;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

public class EVScoreDerivation implements Serializable {
    private static final long serialVersionUID = 7497135843862227141L;

    @JsonProperty("ev")
    private ScoreDerivation ev;

    @JsonProperty("revenue")
    private ScoreDerivation revenue;

    @JsonProperty("probability")
    private ScoreDerivation probability;

    // Serialization constructor.
    public EVScoreDerivation() {
    }

    public EVScoreDerivation(ScoreDerivation ev, ScoreDerivation revenue, ScoreDerivation probability) {
        this.ev = ev;
        this.revenue = revenue;
        this.probability = probability;
    }

    public ScoreDerivation getEVScoreDerivation() {
        return ev;
    }

    public ScoreDerivation getRevenueScoreDerivation() {
        return revenue;
    }

    public ScoreDerivation getProbabilityScoreDerivation() {
        return probability;
    }
}
