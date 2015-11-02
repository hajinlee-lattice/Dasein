package com.latticeengines.domain.exposed.dataflow.flows;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.pls.ProspectDiscoveryConfiguration;
import com.latticeengines.domain.exposed.pls.Quota;
import com.latticeengines.domain.exposed.pls.TargetMarket;

public class QuotaFlowParameters extends DataFlowParameters {
    private List<TargetMarket> targetMarkets;
    private Quota quota;
    private ProspectDiscoveryConfiguration configuration;

    public QuotaFlowParameters(List<TargetMarket> targetMarkets, Quota quota,
            ProspectDiscoveryConfiguration configuration) {
        this.targetMarkets = targetMarkets;
        this.quota = quota;
        this.configuration = configuration;
    }

    @JsonProperty("target_markets")
    public List<TargetMarket> getTargetMarkets() {
        return targetMarkets;
    }

    @JsonProperty("target_markets")
    public void setTargetMarkets(List<TargetMarket> targetMarkets) {
        this.targetMarkets = targetMarkets;
    }

    @JsonProperty("quota")
    public Quota getQuota() {
        return quota;
    }

    @JsonProperty("quota")
    public void setQuota(Quota quota) {
        this.quota = quota;
    }

    @JsonProperty("configuration")
    public ProspectDiscoveryConfiguration getConfiguration() {
        return configuration;
    }

    @JsonProperty("configuration")
    public void setConfiguration(ProspectDiscoveryConfiguration configuration) {
        this.configuration = configuration;
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public QuotaFlowParameters() {
    }

}
