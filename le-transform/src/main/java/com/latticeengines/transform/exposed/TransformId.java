package com.latticeengines.transform.exposed;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

public class TransformId {

    public String modelPath;
    public String moduleName;
    public String version;

    public TransformId(String modelPath, String moduleName, String version) {
        this.modelPath = modelPath;
        this.moduleName = moduleName;
        this.version = version;
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
