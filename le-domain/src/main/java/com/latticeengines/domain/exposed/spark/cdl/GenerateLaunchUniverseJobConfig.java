package com.latticeengines.domain.exposed.spark.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class GenerateLaunchUniverseJobConfig extends SparkJobConfig {
    public static final String NAME = "generateLaunchUniverse";

    @JsonProperty("MaxContactsPerAccount")
    private Long maxContactsPerAccount;

    @JsonProperty("MaxAccountsToLaunch")
    private Long maxAccountsToLaunch;

    @JsonProperty("ContactsPerAccountSortAttribute")
    private String contactsPerAccountSortAttribute;

    @JsonProperty("ContactsPerAccountSortDirection")
    private String contactsPerAccountSortDirection;

    public GenerateLaunchUniverseJobConfig() {
    }

    public GenerateLaunchUniverseJobConfig(String workSpace,
            Long maxContactsPerAccount, Long maxAccountsToLaunch,
            String contactsPerAccountSortAttribute, String contactsPerAccountSortDirection) {
        this.setWorkspace(workSpace);
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

}
