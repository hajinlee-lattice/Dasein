package com.latticeengines.domain.exposed.metadata;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Filters;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.dependency.AbstractDependableObject;
import com.latticeengines.domain.exposed.dependency.Dependable;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@javax.persistence.Table(name = "METADATA_DEPENDABLE", //
uniqueConstraints = { @UniqueConstraint(columnNames = { "TENANT_ID", "NAME", "TYPE" }) })
@Filters({ //
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId") })
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DependableObject extends AbstractDependableObject implements HasPid, HasTenantId {

    public static DependableObject fromDependable(Dependable dependable) {
        DependableObject object = new DependableObject();
        object.setName(dependable.getDependableName());
        object.setType(dependable.getDependableType());

        for (Dependable dependency : dependable.getDependencies()) {
            object.addDependency(dependency);
        }
        return object;
    }

    @JsonProperty("dependencies")
    @Transient
    private List<DependableObject> dependencies = new ArrayList<>();

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "NAME", nullable = false)
    @JsonProperty("name")
    private String name;

    @Column(name = "TYPE", nullable = false)
    @JsonProperty("type")
    @Enumerated(value = EnumType.ORDINAL)
    private DependableType type;

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    private Long tenantId;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public DependableType getType() {
        return type;
    }

    public void setType(DependableType type) {
        this.type = type;
    }

    @JsonIgnore
    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY, mappedBy = "parent")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private List<DependencyLink> dependencyLinks = new ArrayList<>();

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public Tenant getTenant() {
        return tenant;
    }

    public void setTenant(Tenant tenant) {
        this.tenant = tenant;

        if (tenant != null) {
            setTenantId(tenant.getPid());
        }
    }

    @Override
    public Long getTenantId() {
        return tenantId;
    }

    @Override
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    public List<DependencyLink> getDependencyLinks() {
        return dependencyLinks;
    }

    public void setDependencyLinks(List<DependencyLink> dependencyLinks) {
        this.dependencyLinks = dependencyLinks;
    }

    @Override
    public DependableType getDependableType() {
        return getType();
    }

    @Override
    public List<? extends Dependable> getDependencies() {
        return dependencies;
    }

    public void setDependencies(ArrayList<DependableObject> dependencies) {
        this.dependencies = dependencies;
    }

    public void addDependency(Dependable dependency) {
        DependableObject object = new DependableObject();
        object.setName(dependency.getDependableName());
        object.setType(dependency.getDependableType());
        dependencies.add(object);
    }

    @Override
    public String getDependableName() {
        return getName();
    }

    @Override
    public void accept(Visitor visitor, VisitorContext ctx) {
        visitor.visit(this, ctx);
    }

}
