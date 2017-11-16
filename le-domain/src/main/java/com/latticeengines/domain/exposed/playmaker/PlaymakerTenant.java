package com.latticeengines.domain.exposed.playmaker;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@JsonIgnoreProperties(ignoreUnknown = true)
@Entity
@Table(name = "TENANT", uniqueConstraints = { @UniqueConstraint(columnNames = { "TENANT_NAME" }) })
public class PlaymakerTenant implements HasPid {

    private Long pid;
    private String tenantName;
    private String tenantPassword;
    private String externalId;
    private String jdbcDriver;
    private String jdbcUrl;
    private String jdbcUserName;
    private String jdbcPassword;
    private String jdbcPasswordEncrypt;

    public PlaymakerTenant() {
        super();
    }

    public PlaymakerTenant(PlaymakerTenant tenant) {
        this.tenantName = tenant.tenantName;
        this.externalId = tenant.externalId;
        this.jdbcDriver = tenant.jdbcDriver;
        this.jdbcUrl = tenant.jdbcUrl;
        this.jdbcUserName = tenant.jdbcUserName;
        this.jdbcPasswordEncrypt = tenant.jdbcPasswordEncrypt;
    }

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

    @JsonProperty("TenantName")
    @Column(name = "TENANT_NAME", nullable = false)
    public String getTenantName() {
        return tenantName;
    }

    @JsonProperty("TenantName")
    public void setTenantName(String tenantName) {
        this.tenantName = tenantName;
    }

    @Transient
    @JsonProperty("TenantPassword")
    public String getTenantPassword() {
        return tenantPassword;
    }

    @JsonProperty("TenantPassword")
    public void setTenantPassword(String tenantPassword) {
        this.tenantPassword = tenantPassword;
    }

    @JsonProperty("ExternalId")
    @Column(name = "EXTERNAL_ID", nullable = true)
    public String getExternalId() {
        return externalId;
    }

    @JsonProperty("ExternalId")
    public void setExternalId(String externalId) {
        this.externalId = externalId;
    }

    @JsonProperty("JdbcDriver")
    @Column(name = "JDBC_DRIVER", nullable = false)
    public String getJdbcDriver() {
        return jdbcDriver;
    }

    @JsonProperty("JdbcDriver")
    public void setJdbcDriver(String jdbcDriver) {
        this.jdbcDriver = jdbcDriver;
    }

    @JsonProperty("JdbcUrl")
    @Column(name = "JDBC_URL", nullable = false)
    public String getJdbcUrl() {
        return jdbcUrl;
    }

    @JsonProperty("JdbcUrl")
    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    @JsonProperty("JdbcUserName")
    @Column(name = "JDBC_USERNAME", nullable = false)
    public String getJdbcUserName() {
        return jdbcUserName;
    }

    @JsonProperty("JdbcUserName")
    public void setJdbcUserName(String jdbcUserName) {
        this.jdbcUserName = jdbcUserName;
    }

    @JsonProperty("JdbcPassword")
    @Column(name = "JDBC_PASSWORD")
    public String getJdbcPassword() {
        return jdbcPassword;
    }

    @JsonProperty("JdbcPassword")
    public void setJdbcPassword(String jdbcPassword) {
        this.jdbcPassword = jdbcPassword;
    }

    @JsonProperty("JdbcPasswordEncrypt")
    @Column(name = "JDBC_PASSWORD_ENCRYPT", nullable = false)
    public String getJdbcPasswordEncrypt() {
        return jdbcPasswordEncrypt;
    }

    @JsonProperty("JdbcPasswordEncrypt")
    public void setJdbcPasswordEncrypt(String jdbcPasswordEncrypt) {
        this.jdbcPasswordEncrypt = jdbcPasswordEncrypt;
    }

    public void copyFrom(PlaymakerTenant tenant) {
        if (tenant != null) {
            this.tenantName = tenant.getTenantName();
            this.externalId = tenant.getExternalId();
            this.jdbcDriver = tenant.getJdbcDriver();
            this.jdbcUrl = tenant.getJdbcUrl();
            this.jdbcUserName = tenant.getJdbcUserName();
            this.jdbcPasswordEncrypt = tenant.getJdbcPasswordEncrypt();
        }
    }
}
