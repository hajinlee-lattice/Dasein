package com.latticeengines.proxy.exposed.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.network.exposed.metadata.ArtifactInterface;
import com.latticeengines.network.exposed.metadata.MetadataInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("metadataProxy")
public class MetadataProxy extends BaseRestApiProxy implements MetadataInterface, ArtifactInterface {

    public MetadataProxy() {
        super("metadata");
    }

    @Override
    public Boolean createImportTable(String customerSpace, String tableName, Table table) {
        String url = constructUrl("/customerspaces/{customerSpace}/importtables/{tableName}", customerSpace, tableName);
        return post("createImportTable", url, table, Boolean.class);
    }

    @Override
    public List<String> getImportTableNames(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/importtables", customerSpace);
        String[] importTableNames = get("getImportTables", url, String[].class);
        return Arrays.<String> asList(importTableNames);
    }

    public List<Table> getImportTables(String customerSpace) {
        List<String> tableNames = getImportTableNames(customerSpace);
        List<Table> tables = new ArrayList<>();
        for (String tableName : tableNames) {
            Table table = getImportTable(customerSpace, tableName);
            if (table != null) {
                tables.add(table);
            }
        }
        return tables;
    }

    @Override
    public Table getImportTable(String customerSpace, String tableName) {
        String url = constructUrl("/customerspaces/{customerSpace}/importtables/{tableName}", customerSpace, tableName);
        return get("getImportTable", url, Table.class);
    }

    @Override
    public void deleteTable(String customerSpace, String tableName) {
        String url = constructUrl("/customerspaces/{customerSpace}/tables/{tableName}", customerSpace, tableName);
        delete("deleteImportTable", url);
    }

    @Override
    public void deleteImportTable(String customerSpace, String tableName) {
        String url = constructUrl("/customerspaces/{customerSpace}/importtables/{tableName}", customerSpace, tableName);
        delete("deleteImportTable", url);
    }

    @Override
    public void createTable(String customerSpace, String tableName, Table table) {
        String url = constructUrl("/customerspaces/{customerSpace}/tables/{tableName}", customerSpace, tableName);
        post("createTable", url, table, Boolean.class);
    }

    @Override
    public Boolean resetTables(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/tables/reset", customerSpace);
        return post("reset", url, null, Boolean.class);
    }

    @Override
    public void updateTable(String customerSpace, String tableName, Table table) {
        String url = constructUrl("/customerspaces/{customerSpace}/tables/{tableName}", customerSpace, tableName);
        put("updateTable", url, table);
    }

    @Override
    public Table cloneTable(String customerSpace, String tableName) {
        String url = constructUrl("/customerspaces/{customerSpace}/tables/{tableName}/clone", customerSpace, tableName);
        return post("cloneTable", url, null, Table.class);
    }

    @Override
    public Table copyTable(String sourceTenantId, String tableName, String targetTenantId) {
        String url = constructUrl("/customerspaces/{customerSpace}/tables/{tableName}/copy?targetcustomerspace={targetCustomerSpace}", sourceTenantId,
                tableName, targetTenantId);
        return post("copyTable", url, null, Table.class);
    }

    @Override
    public List<String> getTableNames(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/tables", customerSpace);
        String[] importTableNames = get("getTables", url, String[].class);
        return Arrays.<String> asList(importTableNames);
    }

    @Override
    public Table getTable(String customerSpace, String tableName) {
        String url = constructUrl("/customerspaces/{customerSpace}/tables/{tableName}", customerSpace, tableName);
        return get("getTable", url, Table.class);
    }

    public List<Table> getTables(String customerSpace) {
        List<String> tableNames = getTableNames(customerSpace);
        List<Table> tables = new ArrayList<>();
        for (String tableName : tableNames) {
            Table table = getTable(customerSpace, tableName);
            if (table != null) {
                tables.add(table);
            }
        }
        return tables;
    }

    @Override
    public Boolean createArtifact(String customerSpace, String moduleName, String artifactName, Artifact artifact) {
        String url = constructUrl("/customerspaces/{customerSpace}/modules/{moduleName}/artifacts/{artifactName}", //
                customerSpace, moduleName, artifactName);
        return post("createArtifact", url, artifact, Boolean.class);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Artifact> getArtifacts(String customerSpace, String moduleName) {
        String url = constructUrl("/customerspaces/{customerSpace}/modules/{moduleName}", customerSpace, moduleName);
        return get("getArtifacts", url, List.class);
    }

}
