package com.latticeengines.domain.exposed.pls;

import java.util.List;
import java.util.Map;

public class PlayLaunchChannelMap {

    private Map<String, List<LookupIdMap>> uniqueLookupIdMapping;

    private Map<String, PlayLaunchChannel> launchChannelMap;

    public Map<String, List<LookupIdMap>> getUniqueLookupIdMapping() {
        return uniqueLookupIdMapping;
    }

    public void setUniqueLookupIdMapping(Map<String, List<LookupIdMap>> uniqueLookupIdMapping) {
        this.uniqueLookupIdMapping = uniqueLookupIdMapping;
    }

    public Map<String, PlayLaunchChannel> getLaunchChannelMap() {
        return launchChannelMap;
    }

    public void setLaunchChannelMap(Map<String, PlayLaunchChannel> launchChannelMap) {
        this.launchChannelMap = launchChannelMap;
    }

}
