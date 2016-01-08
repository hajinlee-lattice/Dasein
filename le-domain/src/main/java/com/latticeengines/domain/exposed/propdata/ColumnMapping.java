package com.latticeengines.domain.exposed.propdata;

import java.io.Serializable;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "ColumnMapping")
public class ColumnMapping implements HasPid, Serializable {

    private static final long serialVersionUID = 508751868819205316L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "SourceName", nullable = false)
    private String sourceName;

    @Column(name = "SourceColumn", nullable = true)
    private String sourceColumn;

    @Column(name = "Priority", nullable = true)
    private Integer priority;

    @ManyToOne(cascade = { CascadeType.MERGE, CascadeType.REMOVE })
    @JoinColumn(name = "ExternalColumnID", nullable = false)
    private ExternalColumn externalColumn;

    @JsonIgnore
    public Long getPid() { return pid; }

    @JsonIgnore
    public void setPid(Long pid) { this.pid = pid; }

    @JsonProperty("SourceName")
    public String getSourceName() {
        return sourceName;
    }

    @JsonProperty("SourceName")
    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    @JsonProperty("SourceColumn")
    public String getSourceColumn() {
        return sourceColumn;
    }

    @JsonProperty("SourceColumn")
    public void setSourceColumn(String sourceColumn) {
        this.sourceColumn = sourceColumn;
    }

    @JsonProperty("Priority")
    public Integer getPriority() {
        return priority;
    }

    @JsonProperty("Priority")
    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    @JsonIgnore
    public ExternalColumn getExternalColumn() {
        return externalColumn;
    }

    @JsonIgnore
    public void setExternalColumn(ExternalColumn externalColumn) {
        this.externalColumn = externalColumn;
    }
}
