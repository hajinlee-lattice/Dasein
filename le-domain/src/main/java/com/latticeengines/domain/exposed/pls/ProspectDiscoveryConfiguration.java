package com.latticeengines.domain.exposed.pls;

import java.util.ArrayList;
import java.util.List;

public class ProspectDiscoveryConfiguration extends ConfigurationBag {
    @SuppressWarnings("unchecked")
    public ProspectDiscoveryConfiguration(List<ProspectDiscoveryOption> bag) {
        super(List.class.cast(bag));
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
}
