package com.latticeengines.domain.exposed.metadata;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;

import org.hibernate.annotations.Index;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@javax.persistence.Table(name = "METADATA_DEPENDENCY_LINK")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DependencyLink implements HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "CHILD_NAME", nullable = false)
    @JsonProperty("child_name")
    @Index(name = "NAME_TYPE_IDX")
    private String childName;

    @Column(name = "CHILD_TYPE", nullable = false)
    @Enumerated(value = EnumType.ORDINAL)
    @JsonProperty("child_type")
    @Index(name = "NAME_TYPE_IDX")
    private DependableType childType;

    @JsonProperty("parent")
    @ManyToOne
    @JoinColumn(name = "FK_PARENT_ID", nullable = false)
    private DependableObject parent;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getChildName() {
        return childName;
    }

    public void setChildName(String childName) {
        this.childName = childName;
    }

    public DependableType getChildType() {
        return childType;
    }

    public void setChildType(DependableType childType) {
        this.childType = childType;
    }

    public DependableObject getParent() {
        return parent;
    }

    public void setParent(DependableObject parent) {
        this.parent = parent;
    }
}
