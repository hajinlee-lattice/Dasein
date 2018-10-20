package com.latticeengines.domain.exposed.pls;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.db.PropertyBag;

public class ProspectDiscoveryProperty
        extends PropertyBag<ProspectDiscoveryOption, ProspectDiscoveryOptionName> {
    @SuppressWarnings("unchecked")
    public ProspectDiscoveryProperty(List<ProspectDiscoveryOption> bag) {
        super(List.class.cast(bag));
    }

    @SuppressWarnings("unchecked")
    public ProspectDiscoveryProperty() {
        super(List.class.cast(new ArrayList<ProspectDiscoveryOption>()));
    }

    @JsonProperty
    public List<ProspectDiscoveryOption> getBag() {
        return this.bag;
    }

    @JsonProperty
    public void setBag(List<ProspectDiscoveryOption> bag) {
        this.bag = bag;
    }
}
