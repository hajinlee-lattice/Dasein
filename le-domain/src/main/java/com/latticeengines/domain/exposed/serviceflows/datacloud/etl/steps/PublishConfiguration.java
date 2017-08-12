package com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class PublishConfiguration extends BaseStepConfiguration {

    @NotEmptyString
    @NotNull
    private String hdfsPodId;

    @NotNull
    private Publication publication;

    @NotNull
    private PublicationProgress progress;

    @JsonProperty("hdfs_pod_id")
    public String getHdfsPodId() {
        return hdfsPodId;
    }

    @JsonProperty("hdfs_pod_id")
    public void setHdfsPodId(String hdfsPodId) {
        this.hdfsPodId = hdfsPodId;
    }

    @JsonProperty("publication")
    public Publication getPublication() {
        return publication;
    }

    @JsonProperty("publication")
    public void setPublication(Publication publication) {
        this.publication = publication;
    }

    @JsonProperty("progress")
    public PublicationProgress getProgress() {
        return progress;
    }

    @JsonProperty("progress")
    public void setProgress(PublicationProgress progress) {
        this.progress = progress;
    }
}
