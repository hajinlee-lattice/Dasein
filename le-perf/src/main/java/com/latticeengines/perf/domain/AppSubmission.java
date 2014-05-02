package com.latticeengines.perf.domain;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.ApplicationId;

@XmlRootElement(name = "appSubmission")
public class AppSubmission {

    private List<String> applicationIds = new ArrayList<String>();

    public AppSubmission() {
    }

    public AppSubmission(List<ApplicationId> applicationIds) {
        setApplicationIds(applicationIds);
    }

    @XmlElement(name = "applicationId")
    public List<String> getApplicationIds() {
        return applicationIds;
    }

    public void setApplicationIds(List<ApplicationId> appIds) {
        for (ApplicationId appId : appIds) {
            applicationIds.add(appId.toString());
        }
    }
}
