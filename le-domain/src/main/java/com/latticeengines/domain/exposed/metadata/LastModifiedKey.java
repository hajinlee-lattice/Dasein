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

    @Column(name = "LAST_MODIFIED_TS", nullable = false)
    @JsonProperty("last_modified_ts")
    private Long lastModifiedTimeStamp;

    public LastModifiedKey() {
    }

    public LastModifiedKey(List<String> attributes, long lastModifiedTimeStamp) {
        this.setAttributes(attributes);
        this.setLastModifiedTimestamp(lastModifiedTimeStamp);
    }

    public Long getLastModifiedTimestamp() {
        return lastModifiedTimeStamp;
    }

    public void setLastModifiedTimestamp(Long lastModifiedTimeStamp) {
        this.lastModifiedTimeStamp = lastModifiedTimeStamp;
    }

}
