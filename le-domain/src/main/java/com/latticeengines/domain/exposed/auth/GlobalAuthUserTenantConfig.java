package com.latticeengines.domain.exposed.auth;

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
import javax.persistence.ManyToOne;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "GlobalUserTenantConfig")
@NamedQueries({
        @NamedQuery(name = GlobalAuthUserTenantConfig.NQ_FIND_CONFIG_BY_USER_ID, query = GlobalAuthUserTenantConfig.SELECT_USER_CONFIG_SUMMARY
                + "WHERE guc.globalAuthUser.pid = :userId GROUP BY guc.globalAuthTenant.pid"),
        @NamedQuery(name = GlobalAuthUserTenantConfig.NQ_FIND_CONFIG_BY_USER_ID_TENANT_ID, query = GlobalAuthUserTenantConfig.SELECT_USER_CONFIG_SUMMARY
                + "WHERE guc.globalAuthUser.pid = :userId AND guc.globalAuthTenant.pid = :tenantId GROUP BY guc.globalAuthTenant.pid") })
public class GlobalAuthUserTenantConfig extends BaseGlobalAuthObject implements HasPid {

    static final String SELECT_USER_CONFIG_SUMMARY = "SELECT new com.latticeengines.domain.exposed.auth.GlobalAuthUserConfigSummary ( "
            + "guc.globalAuthTenant.id AS tenantDeploymentId, "
            + "MAX ( CASE WHEN guc.configProperty = 'SSO_ENABLED' THEN guc.propertyValue END) AS ssoEnabled, "
            + "MAX ( CASE WHEN guc.configProperty = 'FORCE_SSO_LOGIN' THEN guc.propertyValue END) AS forceSsoLogin "
            + ") " + "FROM GlobalAuthUserTenantConfig guc ";

    public static final String NQ_FIND_CONFIG_BY_USER_ID = "GlobalUserTenantConfig.findConfigByUserId";
    public static final String NQ_FIND_CONFIG_BY_USER_ID_TENANT_ID = "GlobalUserTenantConfig.findConfigByUserIdTenantId";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @JsonIgnore
    @Column(name = "GlobalUserTenantConfig_ID", unique = true, nullable = false)
    private Long userTenantConfigId;

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY)
    @JoinColumn(name = "User_ID", nullable = false, foreignKey = @ForeignKey(name="FK_GAUTCONFIG_GAUSER"))
    @OnDelete(action = OnDeleteAction.CASCADE)
    private GlobalAuthUser globalAuthUser;

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY)
    @JoinColumn(name = "Tenant_ID", nullable = false, foreignKey = @ForeignKey(name="FK_GAUTCONFIG_GATENANT"))
    @OnDelete(action = OnDeleteAction.CASCADE)
    private GlobalAuthTenant globalAuthTenant;

    @JsonProperty("configProperty")
    @Column(name = "Config_Property", nullable = true)
    private String configProperty;

    @JsonProperty("propertyValue")
    @Column(name = "Property_Value", nullable = true)
    private String propertyValue;

    @Override
    public Long getPid() {
        return userTenantConfigId;
    }

    @Override
    public void setPid(Long pid) {
        this.userTenantConfigId = pid;
    }

    public GlobalAuthUser getGlobalAuthUser() {
        return globalAuthUser;
    }

    public void setGlobalAuthUser(GlobalAuthUser globalAuthUser) {
        this.globalAuthUser = globalAuthUser;
    }

    public GlobalAuthTenant getGlobalAuthTenant() {
        return globalAuthTenant;
    }

    public void setGlobalAuthTenant(GlobalAuthTenant globalAuthTenant) {
        this.globalAuthTenant = globalAuthTenant;
    }

    public String getConfigProperty() {
        return configProperty;
    }

    public void setConfigProperty(String configProperty) {
        this.configProperty = configProperty;
    }

    public String getPropertyValue() {
        return propertyValue;
    }

    public void setPropertyValue(String propertyValue) {
        this.propertyValue = propertyValue;
    }

}
