package com.latticeengines.domain.exposed.datacloud.match.entity;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class EntityMatchVersion {
    private final int currentVersion;
    private final int nextVersion;

    @JsonCreator
    public EntityMatchVersion(@JsonProperty("CurrentVersion") int currentVersion,
            @JsonProperty("NextVersion") int nextVersion) {
        this.currentVersion = currentVersion;
        this.nextVersion = nextVersion;
    }

    @JsonProperty("CurrentVersion")
    public int getCurrentVersion() {
        return currentVersion;
    }

    @JsonProperty("NextVersion")
    public int getNextVersion() {
        return nextVersion;
    }

    @Override
    public String toString() {
        return "EntityMatchVersion{" + "currentVersion=" + currentVersion + ", nextVersion=" + nextVersion + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        EntityMatchVersion version = (EntityMatchVersion) o;
        return currentVersion == version.currentVersion && nextVersion == version.nextVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(currentVersion, nextVersion);
    }
}
