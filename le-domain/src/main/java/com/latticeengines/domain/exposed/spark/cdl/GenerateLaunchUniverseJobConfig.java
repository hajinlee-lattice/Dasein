package com.latticeengines.domain.exposed.spark.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class GenerateLaunchUniverseJobConfig extends SparkJobConfig {
    public static final String NAME = "generateLaunchUniverse";

    @JsonProperty("MaxContactsPerAccount")
    private Long maxContactsPerAccount;

    @JsonProperty("MaxEntitiesToLaunch")
    private Long maxEntitiesToLaunch;

    @JsonProperty("ContactsPerAccountSortAttribute")
    private String contactsPerAccountSortAttribute;

    @JsonProperty("ContactsPerAccountSortDirection")
    private String contactsPerAccountSortDirection;

    public GenerateLaunchUniverseJobConfig() {
    }

    public GenerateLaunchUniverseJobConfig(String workSpace,
            Long maxContactsPerAccount, Long maxEntitiesToLaunch,
            String contactsPerAccountSortAttribute, String contactsPerAccountSortDirection) {
        this.setWorkspace(workSpace);
        this.maxContactsPerAccount = maxContactsPerAccount;
        this.maxEntitiesToLaunch = maxEntitiesToLaunch;
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

    public Long getMaxEntitiesToLaunch() {
        return maxEntitiesToLaunch;
    }

    public void setMaxEntitiesToLaunch(Long maxEntitiesToLaunch) {
        this.maxEntitiesToLaunch = maxEntitiesToLaunch;
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
