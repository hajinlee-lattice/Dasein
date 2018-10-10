package com.latticeengines.domain.exposed.scoringapi;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

public class EVFitFunctionParameters implements Serializable {
    private static final long serialVersionUID = 7439595521700906270L;

    @JsonProperty("ev")
    private FitFunctionParameters ev;

    @JsonProperty("revenue")
    private FitFunctionParameters revenue;

    @JsonProperty("probability")
    private FitFunctionParameters probability;

    public EVFitFunctionParameters() {
    }

    public EVFitFunctionParameters(FitFunctionParameters ev,
                                   FitFunctionParameters revenue,
                                   FitFunctionParameters probability) {
        this.ev = ev;
        this.revenue = revenue;
        this.probability = probability;
    }

    public FitFunctionParameters getEVParameters() {
        return ev;
    }

    public FitFunctionParameters getRevenueParameters() {
        return revenue;
    }

    public FitFunctionParameters getProbabilityParameters() {
        return probability;
    }
}
