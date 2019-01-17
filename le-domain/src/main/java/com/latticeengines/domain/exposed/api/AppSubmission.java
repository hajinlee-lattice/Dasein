package com.latticeengines.domain.exposed.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AppSubmission {

    private List<String> applicationIds = new ArrayList<String>();

    public AppSubmission() {
    }

    public AppSubmission(ApplicationId... applicationIds) {
        setIds(Arrays.asList(applicationIds));
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
        if (CollectionUtils.isNotEmpty(appIds)) {
            for (ApplicationId appId : appIds) {
                if (appId != null) {
                    applicationIds.add(appId.toString());
                }
            }
        }
    }
}
