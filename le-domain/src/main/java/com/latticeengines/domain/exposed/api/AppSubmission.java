package com.latticeengines.domain.exposed.api;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AppSubmission {

    private List<String> applicationIds = new ArrayList<String>();

    public AppSubmission() {
    }

    public AppSubmission(List<ApplicationId> applicationIds) {
        setIds(applicationIds);
    }

    @JsonProperty("application_ids")
    public List<String> getApplicationIds() {
        return applicationIds;
    }

    @JsonIgnore
    public void setIds(List<ApplicationId> appIds) {
        for (ApplicationId appId : appIds) {
            applicationIds.add(appId.toString());
        }
    }
}
