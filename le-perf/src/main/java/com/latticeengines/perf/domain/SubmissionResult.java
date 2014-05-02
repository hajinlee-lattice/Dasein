package com.latticeengines.perf.domain;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "result")
public class SubmissionResult {

    private boolean successful;

    public SubmissionResult() {
    }

    public SubmissionResult(boolean successful) {
    	this.successful = successful;
    }

    @XmlElement(name = "success")
    public boolean isSuccessful() {
        return successful;
    }

    public void setSuccessful(boolean successful) {
    	this.successful = successful;
    }
}
