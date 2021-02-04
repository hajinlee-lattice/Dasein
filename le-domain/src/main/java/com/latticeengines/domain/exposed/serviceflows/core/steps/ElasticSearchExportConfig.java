package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ElasticSearchExportConfig {


    @JsonProperty("table_name")
    private String tableName;

    @JsonProperty("signature")
    private String signature;

    @JsonProperty("table_role")
    private TableRoleInCollection tableRoleInCollection;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public TableRoleInCollection getTableRoleInCollection() {
        return tableRoleInCollection;
    }

    public void setTableRoleInCollection(TableRoleInCollection tableRoleInCollection) {
        this.tableRoleInCollection = tableRoleInCollection;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }
}
