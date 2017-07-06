package com.latticeengines.domain.exposed.camille.lifecycle;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Represents the properties describing a service. Offered to camille upon
 * service registration.
 */
public class ServiceProperties extends BaseProperties {
    public ServiceProperties(int dataVersion, String versionString) {
        this.dataVersion = dataVersion;
        this.versionString = versionString;
    }

    public ServiceProperties(int dataVersion) {
        this.dataVersion = dataVersion;
    }

    // Serialization constructor
    public ServiceProperties() {
    }

    public int dataVersion;
    public String versionString;

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
