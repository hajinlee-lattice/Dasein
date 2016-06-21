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
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Filters;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenantId;

@Entity
@Table(name = "METADATA_ARTIFACT", //
    uniqueConstraints = { @UniqueConstraint(columnNames = { "TENANT_ID", "PATH" }) })
@Filters({ @Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId") })
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class Artifact implements HasName, HasPid, HasTenantId {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;
    
    @JsonProperty("name")
    @Column(name = "NAME", nullable = false)
    private String name;
    
    @JsonProperty("path")
    @Column(name = "PATH", nullable = false)
    private String path;
    
    @JsonProperty("type")
    @Column(name = "TYPE", nullable = false)
    private ArtifactType artifactType;
    
    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    private Long tenantId;
    
    @ManyToOne
    @JoinColumn(name = "FK_MODULE_ID", nullable = false)
    private Module module;

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public ArtifactType getArtifactType() {
        return artifactType;
    }

    public void setArtifactType(ArtifactType artifactType) {
        this.artifactType = artifactType;
    }

    @Override
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    @Override
    public Long getTenantId() {
        return tenantId;
    }

    public Module getModule() {
        return module;
    }

    public void setModule(Module module) {
        this.module = module;
    }

}
