package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.activity.CatalogImport;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class BuildCatalogStepConfiguration extends BaseProcessEntityStepConfiguration {

    @JsonProperty("catalog_tables")
    private Map<String, List<CatalogImport>> catalogImports;

    @JsonProperty("entity_match_enabled")
    private boolean entityMatchEnabled;

    @Override
    public BusinessEntity getMainEntity() {
        return BusinessEntity.Catalog;
    }

    public Map<String, List<CatalogImport>> getCatalogImports() {
        return catalogImports;
    }

    public void setCatalogImports(Map<String, List<CatalogImport>> catalogImports) {
        this.catalogImports = catalogImports;
    }

    public boolean isEntityMatchEnabled() {
        return entityMatchEnabled;
    }

    public void setEntityMatchEnabled(boolean entityMatchEnabled) {
        this.entityMatchEnabled = entityMatchEnabled;
    }
}
