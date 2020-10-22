package com.latticeengines.domain.exposed.spark.cdl;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class PublishActivityAlertsJobConfig extends SparkJobConfig {
    public static final String NAME = "publishActivityAlertsJob ";

    @NotNull
    @JsonProperty
    public Map<String, String> alertNameToAlertCategory = new HashMap<>();

    @NotNull
    @JsonProperty
    public String alertVersion;

    @NotNull
    @JsonProperty
    public Long tenantId;

    @JsonProperty
    private String dbTableName;

    @JsonProperty
    private String dbDriver;

    @JsonProperty
    private String dbUrl;

    @JsonProperty
    private String dbUser;

    @JsonProperty
    private String dbPassword;

    @JsonProperty
    private String dbRandomStr;

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return 0;
    }

    public DataUnit getTableToPublish() {
        return getInput().get(0);
    }

    public Map<String, String> getAlertNameToAlertCategory() {
        return alertNameToAlertCategory;
    }

    public void setAlertNameToAlertCategory(Map<String, String> alertNameToAlertCategory) {
        this.alertNameToAlertCategory = alertNameToAlertCategory;
    }

    public String getAlertVersion() {
        return alertVersion;
    }

    public void setAlertVersion(String alertVersion) {
        this.alertVersion = alertVersion;
    }

    public Long getTenantId() {
        return tenantId;
    }

    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    public String getDbTableName() {
        return dbTableName;
    }

    public void setDbTableName(String dbTableName) {
        this.dbTableName = dbTableName;
    }

    public String getDbDriver() {
        return dbDriver;
    }

    public void setDbDriver(String dbDriver) {
        this.dbDriver = dbDriver;
    }

    public String getDbUrl() {
        return dbUrl;
    }

    public void setDbUrl(String dbUrl) {
        this.dbUrl = dbUrl;
    }

    public String getDbUser() {
        return dbUser;
    }

    public void setDbUser(String dbUser) {
        this.dbUser = dbUser;
    }

    public String getDbPassword() {
        return dbPassword;
    }

    public void setDbPassword(String dbPassword) {
        this.dbPassword = dbPassword;
    }

    public String getDbRandomStr() {
        return dbRandomStr;
    }

    public void setDbRandomStr(String dbRandomStr) {
        this.dbRandomStr = dbRandomStr;
    }
}
