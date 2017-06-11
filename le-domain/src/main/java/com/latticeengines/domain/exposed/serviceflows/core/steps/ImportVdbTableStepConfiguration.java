package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.VdbCreateTableRule;
import com.latticeengines.domain.exposed.pls.VdbSpecMetadata;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

import java.util.List;

public class ImportVdbTableStepConfiguration extends BaseStepConfiguration {

    @NotNull
    @JsonProperty("customerSpace")
    private CustomerSpace customerSpace;

    @JsonProperty("collection_identifier")
    private String collectionIdentifier;

    @JsonProperty("import_configuration_str")
    private String importConfigurationStr;


    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }

    public String getCollectionIdentifier() {
        return collectionIdentifier;
    }

    public void setCollectionIdentifier(String collectionIdentifier) {
        this.collectionIdentifier = collectionIdentifier;
    }

    public String getImportConfigurationStr() {
        return importConfigurationStr;
    }

    public void setImportConfigurationStr(String importConfigurationStr) {
        this.importConfigurationStr = importConfigurationStr;
    }

}
