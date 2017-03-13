package com.latticeengines.domain.exposed.metadata;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.hibernate.annotations.Index;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasOptionAndValue;

@Entity
@Table(name = "METADATA_SEGMENT_PROPERTY")
public class MetadataSegmentProperty implements HasOptionAndValue, HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne
    @JoinColumn(name = "METADATA_SEGMENT_ID", nullable = false)
    @JsonIgnore
    private MetadataSegment metadataSegment;

    @Column(name = "PROPERTY", nullable = false)
    @JsonProperty("option")
    @Index(name = "SEGMENT_PROPERTY_IDX")
    private String option;

    @Column(name = "VALUE", nullable = true, length = 2048)
    @JsonProperty("value")
    private String value;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public String getOption() {
        return option;
    }

    @Override
    public void setOption(String option) {
        this.option = option;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public void setValue(String value) {
        this.value = value;
    }

    public MetadataSegment getMetadataSegment() {
        return metadataSegment;
    }

    public void setMetadataSegment(MetadataSegment metadataSegment) {
        this.metadataSegment = metadataSegment;
    }
}
