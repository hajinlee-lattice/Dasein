package com.latticeengines.domain.exposed.camille;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Pod {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private String podId;

    public Pod() {
    }

    public Pod(String podId) {
        this.podId = podId;
    }

    public String getPodId() {
        return podId;
    }

    public void setPodId(String podId) {
        this.podId = podId;
    }

    @Override
    public String toString() {
        return "Pod [podId=" + podId + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((podId == null) ? 0 : podId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof Pod))
            return false;
        final Pod other = (Pod) obj;
        if (podId == null) {
            if (other.podId != null)
                return false;
        } else if (!podId.equals(other.podId))
            return false;
        return true;
    }
}
