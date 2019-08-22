package com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps;

import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class CommitEntityMatchConfiguration extends BaseStepConfiguration {

    @JsonProperty("customer_space")
    private CustomerSpace customerSpace;

    /*
     * Set of entities that will be published. skipPublishedEntities flag will still
     * be honored.
     */
    @Deprecated // always check all relevant entities
    @JsonProperty("entity_list")
    private Set<String> entitySet;

    /*
     * flag to publish any entity that has imports
     */
    @Deprecated // always check all relevant entities
    @JsonProperty("check_all_entity_import")
    private boolean checkAllEntityImport = true;

    /*
     * Set of entities that will be published if there are import for them. If the
     * checkAllEntityImport is set to true, this set will be ignored.
     * skipPublishedEntities flag will still be honored.
     */
    @Deprecated // always check all relevant entities
    @JsonProperty("entity_import_to_check")
    private Set<String> entityImportSetToCheck;

    /*
     * flag to skip all entities that are already published
     */
    @Deprecated // the step only runs once
    @JsonProperty("skip_published_entities")
    private boolean skipPublishedEntities = true;

    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }


    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }

    public Set<String> getEntitySet() {
        return entitySet;
    }

    public void setEntitySet(Set<String> entitySet) {
        this.entitySet = entitySet;
    }

    public boolean isCheckAllEntityImport() {
        return checkAllEntityImport;
    }

    public void setCheckAllEntityImport(boolean checkAllEntityImport) {
        this.checkAllEntityImport = checkAllEntityImport;
    }

    public Set<String> getEntityImportSetToCheck() {
        return entityImportSetToCheck;
    }

    public void setEntityImportSetToCheck(Set<String> entityImportSetToCheck) {
        this.entityImportSetToCheck = entityImportSetToCheck;
    }

    public void addEntity(String entity) {
        if (this.entitySet == null) {
            this.entitySet = new HashSet<>();
        }
        this.entitySet.add(entity);
    }

    public boolean isSkipPublishedEntities() {
        return skipPublishedEntities;
    }

    public void setSkipPublishedEntities(boolean skipPublishedEntities) {
        this.skipPublishedEntities = skipPublishedEntities;
    }
}
