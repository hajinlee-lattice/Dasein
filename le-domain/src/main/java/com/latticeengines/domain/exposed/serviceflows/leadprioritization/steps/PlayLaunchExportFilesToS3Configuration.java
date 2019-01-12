package com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps;

import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;

public class PlayLaunchExportFilesToS3Configuration extends ImportExportS3StepConfiguration {

    private String playName;

    private String playLaunchId;

    public String getPlayName() {
        return playName;
    }

    public void setPlayName(String playName) {
        this.playName = playName;
    }

    public String getPlayLaunchId() {
        return playLaunchId;
    }

    public void setPlayLaunchId(String playLaunchId) {
        this.playLaunchId = playLaunchId;
    }

}
