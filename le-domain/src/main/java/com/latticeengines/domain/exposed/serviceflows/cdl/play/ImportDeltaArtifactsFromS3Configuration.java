package com.latticeengines.domain.exposed.serviceflows.cdl.play;

import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;

public class ImportDeltaArtifactsFromS3Configuration extends ImportExportS3StepConfiguration {
    private String channelId;
    private String playId;
    private String executionId;

    public String getChannelId() { return channelId; }

    public void setChannelId(String channelId) { this.channelId = channelId; }

    public String getPlayId() { return playId; }

    public void setPlayId(String playId) { this.playId = playId; }

    public String getExecutionId() { return executionId; }

    public void setExecutionId(String executionId) { this.executionId = executionId; }
}
