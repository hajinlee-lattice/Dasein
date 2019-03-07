package com.latticeengines.domain.exposed.pls;

import java.util.List;
import java.util.Map;

public class PlayLaunchConfigurations {

    private Map<String, List<LookupIdMap>> uniqueLookupIdMapping;

    private Map<String, PlayLaunch> launchConfigurations;

    public Map<String, List<LookupIdMap>> getUniqueLookupIdMapping() {
        return uniqueLookupIdMapping;
    }

    public void setUniqueLookupIdMapping(Map<String, List<LookupIdMap>> uniqueLookupIdMapping) {
        this.uniqueLookupIdMapping = uniqueLookupIdMapping;
    }

    public Map<String, PlayLaunch> getLaunchConfigurations() {
        return launchConfigurations;
    }

    public void setLaunchConfigurations(Map<String, PlayLaunch> launchConfigurations) {
        this.launchConfigurations = launchConfigurations;
    }

}
