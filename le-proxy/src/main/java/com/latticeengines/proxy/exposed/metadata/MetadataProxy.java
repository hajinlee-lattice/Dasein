package com.latticeengines.proxy.exposed.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.metadata.Module;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.modelreview.ColumnRuleResult;
import com.latticeengines.domain.exposed.modelreview.ModelReviewData;
import com.latticeengines.domain.exposed.modelreview.RowRuleResult;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.network.exposed.metadata.ArtifactInterface;
import com.latticeengines.network.exposed.metadata.MetadataInterface;
import com.latticeengines.network.exposed.metadata.ModuleInterface;
import com.latticeengines.network.exposed.metadata.RuleResultInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("metadataProxy")
public class MetadataProxy extends MicroserviceRestApiProxy implements MetadataInterface, ArtifactInterface,
        RuleResultInterface, ModuleInterface {

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
    public ModelingMetadata getTableMetadata(String customerSpace, String tableName){
        String url = constructUrl("/customerspaces/{customerSpace}/importtables/{tableName}/metadata", customerSpace,
                tableName);
        return get("getTableMetadata", url, ModelingMetadata.class);
    }

    @Override
    public void updateImportTable(String customerSpace, String tableName, Table table){
        String url = constructUrl("/customerspaces/{customerSpace}/importtables/{tableName}", customerSpace, tableName);
        put("updateTable", url, table);
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
        String url = constructUrl(
                "/customerspaces/{customerSpace}/tables/{tableName}/copy?targetcustomerspace={targetCustomerSpace}",
                sourceTenantId, tableName, targetTenantId);
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

    @Override
    public Module getModule(String customerSpace, String moduleName) {
        String url = constructUrl("/customerspaces/{customerSpace}/modules/{moduleName}", customerSpace, moduleName);
        return get("getModule", url, Module.class);
    }

    @Override
    public void validateArtifact(String customerSpace, ArtifactType artifactType, String filePath) {
        String url = constructUrl("/customerspaces/{customerSpace}/artifacttype/{artifactType}?file={filePath}",
                customerSpace, artifactType, filePath);
        post("validateArtifact", url, null, ResponseDocument.class);
    }

    @Override
    public Boolean createColumnResults(List<ColumnRuleResult> results) {
        String url = constructUrl("/ruleresults/column");
        return post("createColumnResults", url, results, Boolean.class);
    }

    @Override
    public Boolean createRowResults(List<RowRuleResult> results) {
        String url = constructUrl("/ruleresults/row");
        return post("createRowResults", url, results, Boolean.class);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<ColumnRuleResult> getColumnResults(String modelId) {
        String url = constructUrl("/ruleresults/column/{modelId}", modelId);
        return get("getColumnResults", url, List.class);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<RowRuleResult> getRowResults(String modelId) {
        String url = constructUrl("/ruleresults/row/{modelId}", modelId);
        return get("getRowResults", url, List.class);
    }

    @Override
    public ModelReviewData getReviewData(String customerSpace, String modelId, String eventTableName) {
        String url = constructUrl("/ruleresults/reviewdata/{customerSpace}/{modelId}/{eventTableName}", customerSpace,
                modelId, eventTableName);
        return get("getReviewData", url, ModelReviewData.class);
    }

    @Override
    public Artifact getArtifactByPath(String customerSpace, String artifactPath) {
        String url = constructUrl("/customerspaces/{customerSpace}/artifactpath?file={artifactPath}", //
                customerSpace, artifactPath);
        return get("getArtifactByPath", url, Artifact.class);
    }

    @Override
    public Boolean provisionImportTables(Tenant tenant) {
        String url = constructUrl("/admin/provision");
        return post("provisionImportTables", url, tenant, Boolean.class);
    }
}
