package com.latticeengines.domain.exposed.spark.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class GenerateLaunchArtifactsJobConfig extends SparkJobConfig {
    public static final String NAME = "generateLaunchArtifacts";

    @JsonProperty("PositiveDelta")
    private DataUnit positiveDelta;
    @JsonProperty("NegativeDelta")
    private DataUnit negativeDelta;
    @JsonProperty("AccountsData")
    private DataUnit accountsData;
    @JsonProperty("ContactsData")
    private DataUnit contactsData;
    @JsonProperty("MainEntity")
    private BusinessEntity mainEntity;

    public GenerateLaunchArtifactsJobConfig() { }

    public GenerateLaunchArtifactsJobConfig(DataUnit accountsData, DataUnit contactsData, DataUnit negativeDelta,
                                            DataUnit positiveDelta, BusinessEntity mainEntity, String workSpace) {
        this.setWorkspace(workSpace);
        this.negativeDelta = negativeDelta;
        this.positiveDelta = positiveDelta;
        this.accountsData = accountsData;
        this.contactsData = contactsData;
        this.mainEntity = mainEntity;
    }

    @Override
    @JsonProperty("Name")
    public String getName() { return NAME; }

    @Override
    public int getNumTargets() { return mainEntity == BusinessEntity.Account ? 3 : 5 ; }

    public DataUnit getPositiveDelta() { return positiveDelta; }

    public void setPositiveDelta(DataUnit positiveDelta) { this.positiveDelta = positiveDelta; }

    public DataUnit getNegativeDelta() { return negativeDelta; }

    public void setNegativeDelta(DataUnit negativeDelta) { this.negativeDelta = negativeDelta; }

    public DataUnit getAccountsData() { return accountsData; }

    public void setAccountsData(DataUnit accountsData) { this.accountsData = accountsData; }

    public DataUnit getContactsData() { return contactsData; }

    public void setContactsData(DataUnit contactsData) { this.contactsData = contactsData; }

    public BusinessEntity getMainEntity() { return mainEntity; }

    public void setMainEntity(BusinessEntity mainEntity) { this.mainEntity = mainEntity; }
}
