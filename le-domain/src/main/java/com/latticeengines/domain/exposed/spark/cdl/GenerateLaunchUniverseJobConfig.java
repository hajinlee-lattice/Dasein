package com.latticeengines.domain.exposed.spark.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class GenerateLaunchUniverseJobConfig extends SparkJobConfig {
    public static final String NAME = "generateLaunchUniverse";

    @JsonProperty("LaunchData")
    private DataUnit launchData;

    @JsonProperty("MaxContactsPerAccount")
    private Long maxContactsPerAccount;

    @JsonProperty("MaxAccountsToLaunch")
    private Long maxAccountsToLaunch;

    @JsonProperty("ContactsPerAccountSortAttribute")
    private String contactsPerAccountSortAttribute;

    @JsonProperty("ContactsPerAccountSortDirection")
    private String contactsPerAccountSortDirection;

    @JsonProperty("ManageDbUrl")
    private String manageDbUrl;

    @JsonProperty("User")
    private String user;

    @JsonProperty("Password")
    private String password;

    @JsonProperty("EncryptionKey")
    private String encryptionKey;

    @JsonProperty("SaltHint")
    private String saltHint;

    public GenerateLaunchUniverseJobConfig() {
    }

    public GenerateLaunchUniverseJobConfig(DataUnit launchData, String workSpace,
            Long maxContactsPerAccount, Long maxAccountsToLaunch,
            String contactsPerAccountSortAttribute, String contactsPerAccountSortDirection) {
        this.setWorkspace(workSpace);
        this.launchData = launchData;
        this.maxContactsPerAccount = maxContactsPerAccount;
        this.maxAccountsToLaunch = maxAccountsToLaunch;
        this.contactsPerAccountSortAttribute = contactsPerAccountSortAttribute;
        this.contactsPerAccountSortDirection = contactsPerAccountSortDirection;
    }

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public DataUnit getLaunchData() {
        return launchData;
    }

    public void setLaunchData(DataUnit launchData) {
        this.launchData = launchData;
    }

    public Long getMaxContactsPerAccount() {
        return maxContactsPerAccount;
    }

    public void setMaxContactsPerAccount(Long maxContactsPerAccount) {
        this.maxContactsPerAccount = maxContactsPerAccount;
    }

    public Long getMaxAccountsToLaunch() {
        return maxAccountsToLaunch;
    }

    public void setMaxAccountsToLaunch(Long maxAccountsToLaunch) {
        this.maxAccountsToLaunch = maxAccountsToLaunch;
    }

    public String getContactsPerAccountSortAttribute() {
        return contactsPerAccountSortAttribute;
    }

    public void setContactsPerAccountSortAttribute(String contactsPerAccountSortAttribute) {
        this.contactsPerAccountSortAttribute = contactsPerAccountSortAttribute;
    }

    public String getContactsPerAccountSortDirection() {
        return contactsPerAccountSortDirection;
    }

    public void setContactsPerAccountSortDirection(String contactsPerAccountSortDirection) {
        this.contactsPerAccountSortDirection = contactsPerAccountSortDirection;
    }

    public String getManageDbUrl() {
        return manageDbUrl;
    }

    public void setManageDbUrl(String manageDbUrl) {
        this.manageDbUrl = manageDbUrl;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getEncryptionKey() {
        return encryptionKey;
    }

    public void setEncryptionKey(String encryptionKey) {
        this.encryptionKey = encryptionKey;
    }

    public String getSaltHint() {
        return saltHint;
    }

    public void setSaltHint(String saltHint) {
        this.saltHint = saltHint;
    }
}
