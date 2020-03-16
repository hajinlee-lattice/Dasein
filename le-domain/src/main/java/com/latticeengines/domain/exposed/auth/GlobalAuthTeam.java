package com.latticeengines.domain.exposed.auth;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.ForeignKey;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "GlobalTeam")
public class GlobalAuthTeam extends BaseGlobalAuthObject implements HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @JsonIgnore
    @Column(name = "GlobalTeam_ID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("team_id")
    @Column(name = "Team_ID", nullable = false)
    private String teamId;

    @JsonProperty("name")
    @Column(name = "Name", nullable = false)
    private String name;

    @JsonProperty("ga_user_tenant_rights")
    @ManyToMany(fetch = FetchType.LAZY, cascade = {CascadeType.MERGE})
    @JoinTable(name = "GlobalTeamTenantMember", joinColumns = {
            @JoinColumn(name = "Team_ID", nullable = false, foreignKey = @ForeignKey(name = "FK_GlobalTeamTenantMember_TeamID_GlobalTeam",
                    foreignKeyDefinition = "foreign key (Team_ID) references GlobalTeam (GlobalTeam_ID) on delete cascade"))}, inverseJoinColumns = {
            @JoinColumn(name = "TenantMember_ID", nullable = false, foreignKey = @ForeignKey(name = "FK_GlobalTeamTenantMember_TenantMemberID_GlobalUserTenantRight",
                    foreignKeyDefinition = "foreign key (TenantMember_ID) references GlobalUserTenantRight (GlobalUserTenantRight_ID) on delete cascade"))})
    private List<GlobalAuthUserTenantRight> gaUserTenantRights = new ArrayList<>();

    @JsonProperty("ga_tenant")
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "Tenant_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private GlobalAuthTenant globalAuthTenant;

    @JsonProperty("created_by_user")
    @Column(name = "Created_By_User")
    private String createdByUser;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public List<GlobalAuthUserTenantRight> getUserTenantRights() {
        return gaUserTenantRights;
    }

    public void setUserTenantRights(List<GlobalAuthUserTenantRight> gaUserTenantRights) {
        this.gaUserTenantRights = gaUserTenantRights;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


    public String getCreatedByUser() {
        return createdByUser;
    }

    public void setCreatedByUser(String createdByUser) {
        this.createdByUser = createdByUser;
    }

    public String getTeamId() {
        return teamId;
    }

    public void setTeamId(String teamId) {
        this.teamId = teamId;
    }

    public GlobalAuthTenant getGlobalAuthTenant() {
        return globalAuthTenant;
    }

    public void setGlobalAuthTenant(GlobalAuthTenant globalAuthTenant) {
        this.globalAuthTenant = globalAuthTenant;
    }
}
