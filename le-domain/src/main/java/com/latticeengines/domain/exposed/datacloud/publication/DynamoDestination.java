package com.latticeengines.domain.exposed.datacloud.publication;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DynamoDestination extends PublicationDestination {

    // Dynamo table version in format
    // {{DataCloudVersion}}_{{DynamoTableSignature}}, eg. 2.0.20,
    // 2.0.20_20191120
    // If not provided, publication of these sources – AccountMaster,
    // AccountMasterDiff, AccountMasterLookup & DunsGuideBook will be populated
    // with a version constructed by current DataCloud version and corresponding
    // dynamo table signature got from LDC_ManageDB.DataCloudVersion table.
    // Other source publication must be provided with Dynamo table version.
    @JsonProperty("Version")
    private String version;

    @Override
    @JsonProperty("DestinationType")
    protected String getDestinationType() {
        return this.getClass().getSimpleName();
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}
