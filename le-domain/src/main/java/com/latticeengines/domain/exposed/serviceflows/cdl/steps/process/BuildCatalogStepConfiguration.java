package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.activity.CatalogImport;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class BuildCatalogStepConfiguration extends BaseProcessEntityStepConfiguration {

    /*-
     * catalogId -> tableName in current active version
     */
    @JsonProperty("catalog_tables")
    private Map<String, String> catalogTables;

    @JsonProperty("catalog_imports")
    private Map<String, List<CatalogImport>> catalogImports;

    @JsonProperty("catalog_ingestion_behaviors")
    private Map<String, DataFeedTask.IngestionBehavior> ingestionBehaviors;

    @JsonProperty("catalog_primary_key_columns")
    private Map<String, String> primaryKeyColumns;

    @JsonProperty("entity_match_enabled")
    private boolean entityMatchEnabled;

    @Override
    public BusinessEntity getMainEntity() {
        return BusinessEntity.Catalog;
    }

    public Map<String, String> getCatalogTables() {
        return catalogTables;
    }

    public void setCatalogTables(Map<String, String> catalogTables) {
        this.catalogTables = catalogTables;
    }

    public Map<String, List<CatalogImport>> getCatalogImports() {
        return catalogImports;
    }

    public void setCatalogImports(Map<String, List<CatalogImport>> catalogImports) {
        this.catalogImports = catalogImports;
    }

    public Map<String, DataFeedTask.IngestionBehavior> getIngestionBehaviors() {
        return ingestionBehaviors;
    }

    public void setIngestionBehaviors(Map<String, DataFeedTask.IngestionBehavior> ingestionBehaviors) {
        this.ingestionBehaviors = ingestionBehaviors;
    }

    public Map<String, String> getPrimaryKeyColumns() {
        return primaryKeyColumns;
    }

    public void setPrimaryKeyColumns(Map<String, String> primaryKeyColumns) {
        this.primaryKeyColumns = primaryKeyColumns;
    }

    public boolean isEntityMatchEnabled() {
        return entityMatchEnabled;
    }

    public void setEntityMatchEnabled(boolean entityMatchEnabled) {
        this.entityMatchEnabled = entityMatchEnabled;
    }
}
