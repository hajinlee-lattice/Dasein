package com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.pls.ProspectDiscoveryProperty;
import com.latticeengines.domain.exposed.pls.Quota;
import com.latticeengines.domain.exposed.pls.TargetMarket;

public class QuotaFlowParameters extends DataFlowParameters {
    private TargetMarket targetMarket;
    private Quota quota;
    private ProspectDiscoveryProperty configuration;

    public QuotaFlowParameters(TargetMarket targetMarket, Quota quota, ProspectDiscoveryProperty configuration) {
        this.targetMarket = targetMarket;
        this.quota = quota;
        this.configuration = configuration;
        this.noFlink = true;
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
    public ProspectDiscoveryProperty getConfiguration() {
        return configuration;
    }

    @JsonProperty("configuration")
    public void setConfiguration(ProspectDiscoveryProperty configuration) {
        this.configuration = configuration;
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public QuotaFlowParameters() {
    }

}
