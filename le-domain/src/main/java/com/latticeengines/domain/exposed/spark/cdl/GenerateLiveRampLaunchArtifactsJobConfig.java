package com.latticeengines.domain.exposed.spark.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class GenerateLiveRampLaunchArtifactsJobConfig extends SparkJobConfig {
    public static final String NAME = "generateLiveRampLaunchArtifacts";

    @JsonProperty("AddContacts")
    private DataUnit addContacts;

    @JsonProperty("RemoveContacts")
    private DataUnit removeContacts;

    public GenerateLiveRampLaunchArtifactsJobConfig() {
    }

    public GenerateLiveRampLaunchArtifactsJobConfig(String workSpace, DataUnit addContacts, DataUnit removeContacts) {
        this.setWorkspace(workSpace);
        this.addContacts = addContacts;
        this.removeContacts = removeContacts;
    }

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return 2;
    }

    public DataUnit getAddContacts() {
        return addContacts;
    }

    public void setAddContacts(DataUnit addContacts) {
        this.addContacts = addContacts;
    }

    public DataUnit getRemoveContacts() {
        return removeContacts;
    }

    public void setRemoveContacts(DataUnit removeContacts) {
        this.removeContacts = removeContacts;
    }
}
