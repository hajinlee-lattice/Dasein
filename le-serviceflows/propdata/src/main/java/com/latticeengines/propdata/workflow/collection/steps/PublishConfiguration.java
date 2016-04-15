package com.latticeengines.propdata.workflow.collection.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.propdata.publication.PublicationConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class PublishConfiguration extends BaseStepConfiguration {

    @NotEmptyString
    @NotNull
    private String hdfsPodId;

    @NotNull
    private PublicationConfiguration publicationConfiguration;

    @JsonProperty("hdfs_pod_id")
    public String getHdfsPodId() {
        return hdfsPodId;
    }

    @JsonProperty("hdfs_pod_id")
    public void setHdfsPodId(String hdfsPodId) {
        this.hdfsPodId = hdfsPodId;
    }

    @JsonProperty("pub_config")
    public PublicationConfiguration getPublicationConfiguration() {
        return publicationConfiguration;
    }

    @JsonProperty("pub_config")
    public void setPublicationConfiguration(PublicationConfiguration publicationConfiguration) {
        this.publicationConfiguration = publicationConfiguration;
    }
}
