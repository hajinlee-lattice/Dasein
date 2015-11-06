package com.latticeengines.domain.exposed.pls;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ProspectDiscoveryConfiguration extends ConfigurationBag<ProspectDiscoveryOption> {
    @SuppressWarnings("unchecked")
    public ProspectDiscoveryConfiguration(List<ProspectDiscoveryOption> bag) {
        super(List.class.cast(bag));
    }

    @JsonProperty 
    public List<ProspectDiscoveryOption> getBag() {
        return this.bag;
    }
    
    @JsonProperty 
    public void setBag(List<ProspectDiscoveryOption> bag) {
        this.bag = bag;
    }

    @SuppressWarnings("unchecked")
    public ProspectDiscoveryConfiguration() {
        super(List.class.cast(new ArrayList<ProspectDiscoveryOption>()));
    }

    public String getString(ProspectDiscoveryOptionName option, String dflt) {
        return super.getString(option.toString(), dflt);
    }

    public int getInt(ProspectDiscoveryOptionName option, int dflt) {
        return super.getInt(option.toString(), dflt);
    }

    public double getDouble(ProspectDiscoveryOptionName option, double dflt) {
        return super.getDouble(option.toString(), dflt);
    }

    public void setString(ProspectDiscoveryOptionName option, String value) {
        super.setString(option.toString(), value);
    }

    public void setInt(ProspectDiscoveryOptionName option, int value) {
        super.setInt(option.toString(), value);
    }

    public void setDouble(ProspectDiscoveryOptionName option, double value) {
        super.setDouble(option.toString(), value);
    }

}
