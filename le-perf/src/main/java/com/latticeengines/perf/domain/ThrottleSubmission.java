package com.latticeengines.perf.domain;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "throttleSubmission")
public class ThrottleSubmission {

    private boolean immediate;
    
    public ThrottleSubmission() {
    }
    
    public ThrottleSubmission(boolean immediate) {
        this.immediate = immediate;
    }

    @XmlElement(name = "immediate")
    public boolean isImmediate() {
        return immediate;
    }
    
    public void setImmediate(boolean immediate) {
        this.immediate = immediate;
    }

}
