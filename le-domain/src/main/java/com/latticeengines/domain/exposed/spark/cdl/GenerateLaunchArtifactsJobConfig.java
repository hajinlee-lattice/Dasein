package com.latticeengines.domain.exposed.spark.cdl;

import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class GenerateLaunchArtifactsJobConfig extends SparkJobConfig {
    public static final String NAME = "generateLaunchArtifacts";
    @JsonProperty("MainEntity")
    private BusinessEntity mainEntity;

    @JsonProperty("IncludeAccountsWithoutContacts")
    private boolean includeAccountsWithoutContacts;

    @JsonProperty("ExternalSystemName")
    private CDLExternalSystemName externalSystemName;

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

    @JsonProperty("accountAttributes")
    private Set<String> accountAttributes = new HashSet<>();

    @JsonProperty("contactAttributes")
    private Set<String> contactAttributes = new HashSet<>();

    public GenerateLaunchArtifactsJobConfig() {
    }

    public GenerateLaunchArtifactsJobConfig(DataUnit accountsData, //
            DataUnit contactsData, DataUnit targetSegmentsContactsData, //
            DataUnit negativeDelta, DataUnit positiveDelta, //
            BusinessEntity mainEntity, boolean includeAccountsWithoutContacts, //
            String workSpace, CDLExternalSystemName externalSystemName, DataUnit perAccountLimitedContacts,
            boolean useContactsPerAccountLimit) {
        this.setWorkspace(workSpace);
        this.setInput(Lists.newArrayList(accountsData, contactsData, targetSegmentsContactsData, negativeDelta, positiveDelta, perAccountLimitedContacts));
        this.mainEntity = mainEntity;
        this.includeAccountsWithoutContacts = includeAccountsWithoutContacts;
        this.externalSystemName = externalSystemName;
        this.useContactsPerAccountLimit = useContactsPerAccountLimit
    }

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return mainEntity == BusinessEntity.Account ? 3 : 5;
    }

    public BusinessEntity getMainEntity() {
        return mainEntity;
    }

    public void setMainEntity(BusinessEntity mainEntity) {
        this.mainEntity = mainEntity;
    }

    public boolean isIncludeAccountsWithoutContacts() {
        return includeAccountsWithoutContacts;
    }

    public void setIncludeAccountsWithoutContacts(boolean includeAccountsWithoutContacts) {
        this.includeAccountsWithoutContacts = includeAccountsWithoutContacts;
    }

    public boolean useContactsPerAccountLimit() {
        return useContactsPerAccountLimit;
    }

    public void setUseContactsPerAccountLimit(boolean useContactsPerAccountLimit) {
        return this.useContactsPerAccountLimit = useContactsPerAccountLimit;
    }

    public void setExternalSystemName(CDLExternalSystemName externalSystemName) {
        this.externalSystemName = externalSystemName;
    }

    public CDLExternalSystemName getExternalSystemName() {
        return externalSystemName;
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

    public Set<String> getContactAttributes() {
        return contactAttributes;
    }

    public void setContactAttributes(Set<String> contactAttributes) {
        this.contactAttributes = contactAttributes;
    }

    public Set<String> getAccountAttributes() {
        return accountAttributes;
    }

    public void setAccountAttributes(Set<String> accountAttributes) {
        this.accountAttributes = accountAttributes;
    }
}
