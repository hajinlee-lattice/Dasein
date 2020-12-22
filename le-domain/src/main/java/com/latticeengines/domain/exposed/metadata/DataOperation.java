package com.latticeengines.domain.exposed.metadata;

import java.util.Date;

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

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.DataOperationConfiguration;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@javax.persistence.Table(name = "DATA_OPERATION")
@Filter(name = "tenantFilter", condition = "FK_TENANT_ID = :tenantFilterId")
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class DataOperation implements HasPid {

    private static final Logger log = LoggerFactory.getLogger(DataOperation.class);

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne(cascade = {CascadeType.MERGE}, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @JsonProperty("operationType")
    @Column(name = "OPERATION_TYPE")
    @Enumerated(EnumType.STRING)
    private OperationType OperationType;

    @JsonProperty("dropPath")
    @Column(name = "DROP_PATH")
    private String dropPath;

    @JsonProperty("configuration")
    @Column(name = "CONFIGURATION", columnDefinition = "'JSON'")
    @Type(type = "json")
    private DataOperationConfiguration configuration;

    @JsonProperty("createDate")
    @Column(name = "CREATE_DATE")
    private Date createDate;

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
    }

    public DataOperation.OperationType getOperationType() {
        return OperationType;
    }

    public void setOperationType(DataOperation.OperationType operationType) {
        OperationType = operationType;
    }

    public String getDropPath() {
        return dropPath;
    }

    public void setDropPath(String dropPath) {
        this.dropPath = dropPath;
    }

    public DataOperationConfiguration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(DataOperationConfiguration configuration) {
        this.configuration = configuration;
    }

    public Date getCreateDate() {
        return createDate;
    }

    public void setCreateDate(Date createDate) {
        this.createDate = createDate;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public enum OperationType{
        DELETE,
        IMPORT;
    }

}
