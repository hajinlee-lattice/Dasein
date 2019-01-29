package com.latticeengines.domain.exposed.pls;

import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.NamedQuery;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "EXTERNAL_SYSTEM_AUTHENTICATION")
@NamedQuery(name = ExternalSystemAuthentication.NQ_FIND_AUTHS_BY_LOOKUPMAP_IDS, 
                query = ExternalSystemAuthentication.SELECT_AUTHS_BY_LOOKUPMAP_IDS)
@NamedQuery(name = ExternalSystemAuthentication.NQ_FIND_AUTHS_BY_AUTH_ID, 
                query = ExternalSystemAuthentication.SELECT_AUTHS_BY_AUTH_ID)
@NamedQuery(name = ExternalSystemAuthentication.NQ_FIND_ALL_AUTHS, 
                query = ExternalSystemAuthentication.SELECT_ALL_AUTHS)
@Filter(name = "tenantFilter", condition = "FK_TENANT_ID = :tenantFilterId")
public class ExternalSystemAuthentication implements HasPid, HasId<String>, HasTenant, HasAuditingFields {

    public static final String NQ_FIND_AUTHS_BY_LOOKUPMAP_IDS = "ExternalSystemAuthentication.findAuthsByLookupMapConfigId";
    public static final String NQ_FIND_AUTHS_BY_AUTH_ID = "ExternalSystemAuthentication.findAuthsByConfigId";
    public static final String NQ_FIND_ALL_AUTHS = "ExternalSystemAuthentication.findAllAuths";
    static final String SELECT_AUTHS_BY_LOOKUPMAP_IDS = "SELECT new com.latticeengines.domain.exposed.pls.ExternalSystemAuthentication "
            + "( esa, esa.lookupIdMap.id ) "
            + "FROM ExternalSystemAuthentication esa WHERE esa.lookupIdMap.id in :lookupMapIds";
    static final String SELECT_AUTHS_BY_AUTH_ID = "SELECT new com.latticeengines.domain.exposed.pls.ExternalSystemAuthentication "
            + "( esa, esa.lookupIdMap.id ) "
            + "FROM ExternalSystemAuthentication esa WHERE esa.id = :authId";
    static final String SELECT_ALL_AUTHS = "SELECT new com.latticeengines.domain.exposed.pls.ExternalSystemAuthentication "
            + "( esa, esa.lookupIdMap.id ) "
            + "FROM ExternalSystemAuthentication esa";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("authId")
    @Column(name = "ID", unique = true, nullable = false)
    private String id;

    @JsonIgnore
    @OneToOne(cascade = CascadeType.MERGE, fetch = FetchType.LAZY)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JoinColumn(name = "FK_LOOKUP_ID_MAP", nullable = false)
    private LookupIdMap lookupIdMap;

    @JsonProperty("lookupMapConfigId")
    @Transient
    private String lookupMapConfigId;

    @JsonProperty("trayAuthenticationId")
    @Column(name = "TRAY_AUTHENTICATION_ID", nullable = true)
    private String trayAuthenticationId;

    @JsonIgnore
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @JsonProperty("created")
    @Column(name = "CREATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date created;

    @JsonProperty("updated")
    @Column(name = "UPDATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date updated;

    public ExternalSystemAuthentication() {
    }

    public ExternalSystemAuthentication(ExternalSystemAuthentication extSysAuth, String lookupMapConfigId) {
        this.pid = extSysAuth.getPid();
        this.id = extSysAuth.getId();
        this.trayAuthenticationId = extSysAuth.getTrayAuthenticationId();
        this.lookupMapConfigId = lookupMapConfigId;
        this.lookupIdMap = extSysAuth.getLookupIdMap();
        this.created = extSysAuth.getCreated();
        this.updated = extSysAuth.getUpdated();
        this.tenant = extSysAuth.getTenant();
    }

    public Long getPid() {
        return pid;
    }

    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Tenant getTenant() {
        return tenant;
    }

    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    public Date getUpdated() {
        return updated;
    }

    public void setUpdated(Date updated) {
        this.updated = updated;
    }


    public String getLookupMapConfigId() {
        return lookupMapConfigId;
    }

    public void setLookupMapConfigId(String lookupMapConfigId) {
        this.lookupMapConfigId = lookupMapConfigId;
    }

    public String getTrayAuthenticationId() {
        return trayAuthenticationId;
    }

    public void setTrayAuthenticationId(String trayAuthenticationId) {
        this.trayAuthenticationId = trayAuthenticationId;
    }

    public LookupIdMap getLookupIdMap() {
        return lookupIdMap;
    }

    public void setLookupIdMap(LookupIdMap lookupIdMap) {
        this.lookupIdMap = lookupIdMap;
    }

}
