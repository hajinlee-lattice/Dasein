package com.latticeengines.domain.exposed.cdl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "CDL_EXTERNAL_SYSTEM", uniqueConstraints = { @UniqueConstraint(columnNames = { "TENANT_ID" }) })
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
public class CDLExternalSystem implements HasPid, HasTenant, HasTenantId {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonProperty("tenant")
    private Tenant tenant;

    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    private Long tenantId;

    @JsonProperty("crm_ids")
    @Column(name = "CRM_IDS", length = 4000)
    private String crmIds;

    @JsonProperty("map_ids")
    @Column(name = "MAP_IDS", length = 4000)
    private String mapIds;

    @JsonProperty("erp_ids")
    @Column(name = "ERP_IDS", length = 4000)
    private String erpIds;

    @JsonProperty("other_ids")
    @Column(name = "OTHER_IDS", length = 4000)
    private String otherIds;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    @Override
    public Long getTenantId() {
        return tenantId;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
        if (tenant != null) {
            setTenantId(tenant.getPid());
        }
    }

    @Override
    public Tenant getTenant() {
        return tenant;
    }

    public String getCrmIds() {
        return crmIds;
    }

    public void setCrmIds(String crmIds) {
        this.crmIds = crmIds;
    }

    public String getMapIds() {
        return mapIds;
    }

    public void setMapIds(String mapIds) {
        this.mapIds = mapIds;
    }

    public String getErpIds() {
        return erpIds;
    }

    public void setErpIds(String erpIds) {
        this.erpIds = erpIds;
    }

    public String getOtherIds() {
        return otherIds;
    }

    public void setOtherIds(String otherIds) {
        this.otherIds = otherIds;
    }

    @JsonIgnore
    @Transient
    public List<String> getCRMIdList() {
        if (StringUtils.isEmpty(crmIds)) {
            return new ArrayList<>();
        } else {
            return Arrays.asList(crmIds.split("\\s*,\\s*"));
        }
    }

    @JsonIgnore
    @Transient
    public void setCRMIdList(List<String> idList) {
        if (idList == null || idList.size() == 0) {
            crmIds = "";
        } else {
            crmIds = String.join(",", idList);
        }
    }

    @JsonIgnore
    @Transient
    public List<String> getMAPIdList() {
        if (StringUtils.isEmpty(mapIds)) {
            return new ArrayList<>();
        } else {
            return Arrays.asList(mapIds.split("\\s*,\\s*"));
        }
    }

    @JsonIgnore
    @Transient
    public void setMAPIdList(List<String> idList) {
        if (idList == null || idList.size() == 0) {
            mapIds = "";
        } else {
            mapIds = String.join(",", idList);
        }
    }

    @JsonIgnore
    @Transient
    public List<String> getERPIdList() {
        if (StringUtils.isEmpty(erpIds)) {
            return new ArrayList<>();
        } else {
            return Arrays.asList(erpIds.split("\\s*,\\s*"));
        }
    }

    @JsonIgnore
    @Transient
    public void setERPIdList(List<String> idList) {
        if (idList == null || idList.size() == 0) {
            erpIds = "";
        } else {
            erpIds = String.join(",", idList);
        }
    }

    @JsonIgnore
    @Transient
    public List<String> getOtherIdList() {
        if (StringUtils.isEmpty(otherIds)) {
            return new ArrayList<>();
        } else {
            return Arrays.asList(otherIds.split("\\s*,\\s*"));
        }
    }

    @JsonIgnore
    @Transient
    public void setOtherIdList(List<String> idList) {
        if (idList == null || idList.size() == 0) {
            otherIds = "";
        } else {
            otherIds = String.join(",", idList);
        }
    }
}
