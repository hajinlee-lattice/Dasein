package com.latticeengines.domain.exposed.serviceflows.cdl.play;

import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;

public class ImportDeltaCalculationResultsFromS3StepConfiguration extends ImportExportS3StepConfiguration {

    private String playId;
    private String launchId;

    public String getPlayId() {
        return playId;
    }

    public void setPlayId(String playId) {
        this.playId = playId;
    }

    public String getLaunchId() {
        return this.launchId;
    }

    public void setLaunchId(String launchId) {
        this.launchId = launchId;
    }

}
