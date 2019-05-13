package com.latticeengines.domain.exposed.pls;

import java.util.List;

public class PlayLaunchChannelSummary {

    private List<PlayLaunchChannel> launchChannelList;

    public List<PlayLaunchChannel> getLaunchChannelMap() {
        return launchChannelList;
    }

    public void setLaunchChannelMap(List<PlayLaunchChannel> launchChannelList) {
        this.launchChannelList = launchChannelList;
    }

}
