package com.latticeengines.domain.exposed.pls;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.db.PropertyBag;

public class TargetMarketDataFlowProperty extends PropertyBag<TargetMarketDataFlowOption, TargetMarketDataFlowOptionName> {
    @SuppressWarnings("unchecked")
    public TargetMarketDataFlowProperty(List<TargetMarketDataFlowOption> bag) {
        super(List.class.cast(bag));
    }

    @SuppressWarnings("unchecked")
    public TargetMarketDataFlowProperty() {
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
