package com.latticeengines.domain.exposed.pls;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TargetMarketDataFlowConfiguration extends ConfigurationBag<TargetMarketDataFlowOption, TargetMarketDataFlowOptionName> {
    @SuppressWarnings("unchecked")
    public TargetMarketDataFlowConfiguration(List<TargetMarketDataFlowOption> bag) {
        super(List.class.cast(bag));
    }

    @SuppressWarnings("unchecked")
    public TargetMarketDataFlowConfiguration() {
        super(List.class.cast(new ArrayList<TargetMarketDataFlowOption>()));
    }

    @JsonProperty
    public List<TargetMarketDataFlowOption> getBag() {
        return this.bag;
    }

    @JsonProperty
    public void setBag(List<TargetMarketDataFlowOption> bag) {
        this.bag = bag;
    }
}
