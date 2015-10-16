package com.latticeengines.domain.exposed.metadata;

import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@Entity
@javax.persistence.Table(name = "METADATA_LASTMODIFIED_KEY")
@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
public class LastModifiedKey extends AttributeOwner {

    private Long lastModifiedTimeStemp;

    public LastModifiedKey() {
    }

    public LastModifiedKey(List<String> attributes, long lastModifiedTimeStemp) {
        this.setAttributes(attributes);
        this.setLastModifiedTimestamp(lastModifiedTimeStemp);
    }

    @Column(name = "LAST_MODIFIED_TS", nullable = false)
    @JsonProperty("last_modified_ts")
    public Long getLastModifiedTimestamp() {
        return lastModifiedTimeStemp;
    }

    @JsonProperty("last_modified_ts")
    public void setLastModifiedTimestamp(Long lastModifiedTimeStemp) {
        this.lastModifiedTimeStemp = lastModifiedTimeStemp;
    }

}
