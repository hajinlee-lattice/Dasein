package com.latticeengines.domain.exposed.scoringapi;

import java.io.Serializable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;

public class FitFunctionParameters implements Serializable {

    private static final long serialVersionUID = -1117763326097592033L;

    @JsonProperty("alpha")
    private double alpha;

    @JsonProperty("beta")
    private double beta;

    @JsonProperty("gamma")
    private double gamma;

    @JsonProperty("maxRate")
    private double maxRate;

    @JsonProperty("version")
    private String version;

    public FitFunctionParameters() {
    }

    public FitFunctionParameters(double alpha, double beta, double gamma, double maxRate, String version) {
        this.alpha = alpha;
        this.beta = beta;
        this.gamma = gamma;
        this.maxRate = maxRate;
        this.version = version;
    }

    public double getAlpha() {
        return alpha;
    }

    public double getBeta() {
        return beta;
    }

    public double getGamma() {
        return gamma;
    }

    public double getMaxRate() {
        return maxRate;
    }

    public String getVersion() {
        return version;
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }
}
