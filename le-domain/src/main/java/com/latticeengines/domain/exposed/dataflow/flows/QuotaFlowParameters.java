package com.latticeengines.domain.exposed.dataflow.flows;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.pls.ProspectDiscoveryConfiguration;
import com.latticeengines.domain.exposed.pls.Quota;
import com.latticeengines.domain.exposed.pls.TargetMarket;

public class QuotaFlowParameters extends DataFlowParameters {
    private TargetMarket targetMarket;
    private Quota quota;
    private ProspectDiscoveryConfiguration configuration;

    public QuotaFlowParameters(TargetMarket targetMarket, Quota quota, ProspectDiscoveryConfiguration configuration) {
        this.targetMarket = targetMarket;
        this.quota = quota;
        this.configuration = configuration;
    }

    @JsonProperty("target_market")
    public TargetMarket getTargetMarket() {
        return targetMarket;
    }

    @JsonProperty("target_market")
    public void setTargetMarket(TargetMarket targetMarket) {
        this.targetMarket = targetMarket;
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
