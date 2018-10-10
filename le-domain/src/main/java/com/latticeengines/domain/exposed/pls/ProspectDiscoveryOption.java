package com.latticeengines.domain.exposed.pls;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

import org.hibernate.annotations.Filter;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasOptionAndValue;
import com.latticeengines.domain.exposed.security.HasTenantId;

@Entity
@Table(name = "PROSPECT_DISCOVERY_OPTION", indexes = { //
        @Index(name = "PROSPECT_DISCOVERY_OPTION_TENANT_ID_IDX", columnList = "TENANT_ID"), //
        @Index(name = "PROSPECT_DISCOVERY_OPTION_OPTION_IDX", columnList = "OPTION") //
})
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
public class ProspectDiscoveryOption implements HasTenantId, HasOptionAndValue, HasPid {

    private Long pid;
    private Long tenantId;
    private String option;
    private String value;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    public Long getTenantId() {
        return tenantId;
    }

    @Override
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    @Column(name = "OPTION", nullable = false)
    @JsonProperty("option")
    public String getOption() {
        return option;
    }

    public void setOption(String option) {
        this.option = option;
    }

    @Column(name = "VALUE")
    @JsonProperty("value")
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

}
