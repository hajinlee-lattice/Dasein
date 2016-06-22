package com.latticeengines.domain.exposed.auth;

import java.util.Date;
import java.util.List;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "GlobalTenant")
public class GlobalAuthTenant implements HasName, HasId<String>, HasPid {

    @JsonProperty("deployment_id")
    @Column(name = "Deployment_ID", nullable = true, unique = true)
    private String id;

    @JsonProperty("display_name")
    @Column(name = "Display_Name", nullable = true)
    private String name;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @JsonIgnore
    @Column(name = "GlobalTenant_ID", nullable = false, unique = true)
    private Long pid;

    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER, mappedBy = "globalAuthTenant")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private List<GlobalAuthUserTenantRight> gaUserTenantRights;

    @JsonProperty("creation_date")
    @Column(name = "Creation_Date", nullable = false)
    private Date creationDate;

    @JsonProperty("last_modification_date")
    @Column(name = "Last_Modification_Date", nullable = false)
    private Date lastModificationDate;

    public GlobalAuthTenant() {
        creationDate = new Date(System.currentTimeMillis());
        lastModificationDate = new Date(System.currentTimeMillis());
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;

    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    public List<GlobalAuthUserTenantRight> getUserTenantRights() {
        return gaUserTenantRights;
    }

    public void setUserTenantRights(List<GlobalAuthUserTenantRight> gaUserTenantRights) {
        this.gaUserTenantRights = gaUserTenantRights;
    }

    public Date getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
    }

    public Date getLastModificationDate() {
        return lastModificationDate;
    }

    public void setLastModificationDate(Date lastModificationDate) {
        this.lastModificationDate = lastModificationDate;
    }
}
