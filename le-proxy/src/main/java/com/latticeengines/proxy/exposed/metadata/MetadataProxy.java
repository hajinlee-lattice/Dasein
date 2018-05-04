package com.latticeengines.proxy.exposed.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.cache.annotation.CacheConfig;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.Module;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.modelreview.ColumnRuleResult;
import com.latticeengines.domain.exposed.modelreview.ModelReviewData;
import com.latticeengines.domain.exposed.modelreview.RowRuleResult;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("metadataProxy")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
@CacheConfig(cacheNames = CacheName.Constants.MetadataCacheName)
public class MetadataProxy extends MicroserviceRestApiProxy {

    public MetadataProxy() {
        super("metadata");
    }

    public Boolean createImportTable(String customerSpace, String tableName, Table table) {
        String url = constructUrl("/customerspaces/{customerSpace}/importtables/{tableName}", customerSpace, tableName);
        return post("createImportTable", url, table, Boolean.class);
    }

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

    public Table getImportTable(String customerSpace, String tableName) {
        String url = constructUrl("/customerspaces/{customerSpace}/importtables/{tableName}", customerSpace, tableName);
        return get("getImportTable", url, Table.class);
    }

    public void deleteTable(String customerSpace, String tableName) {
        String url = constructUrl("/customerspaces/{customerSpace}/tables/{tableName}", customerSpace, tableName);
        delete("deleteTable", url);
    }

    public void deleteImportTable(String customerSpace, String tableName) {
        String url = constructUrl("/customerspaces/{customerSpace}/importtables/{tableName}", customerSpace, tableName);
        delete("deleteImportTable", url);
    }

    public ModelingMetadata getTableMetadata(String customerSpace, String tableName) {
        String url = constructUrl("/customerspaces/{customerSpace}/importtables/{tableName}/metadata", customerSpace,
                tableName);
        return get("getTableMetadata", url, ModelingMetadata.class);
    }

    public void updateImportTable(String customerSpace, String tableName, Table table) {
        String url = constructUrl("/customerspaces/{customerSpace}/importtables/{tableName}", customerSpace, tableName);
        put("updateTable", url, table);
    }

    public void createTable(String customerSpace, String tableName, Table table) {
        String url = constructUrl("/customerspaces/{customerSpace}/tables/{tableName}", customerSpace, tableName);
        List<Attribute> attributes = null;
        try {
            if (table.getAttributes() != null && table.getAttributes().size() > 4000) {
                attributes = table.getAttributes();
                table.setAttributes(null);
            }
            post("createTable", url, table, null);
            addTableAttributes(customerSpace, tableName, attributes);
        } catch(Exception e) {
            deleteTable(customerSpace, tableName);
            throw e;
        } finally {
            if (attributes != null) {
                table.setAttributes(attributes);
            }
        }
    }

    private void addTableAttributes(String customerSpace, String tableName, List<Attribute> attributes) {
        if (attributes == null) {
            return;
        }
        String url = constructUrl("/customerspaces/{customerSpace}/tables/{tableName}/attributes", customerSpace, tableName);
        int chunkSize = 4000;
        for (int i = 0; (i * chunkSize) < attributes.size(); i++) {
            List<Attribute> subList = attributes.subList(i * chunkSize, Math.min(i * chunkSize + chunkSize, attributes.size()));
            post("addTableAttributes", url, subList, null);
        }
    }

    public Boolean resetTables(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/tables/reset", customerSpace);
        return post("reset", url, null, Boolean.class);
    }

    public void updateTable(String customerSpace, String tableName, Table table) {
        String url = constructUrl("/customerspaces/{customerSpace}/tables/{tableName}", customerSpace, tableName);
        List<Attribute> attributes = null;
        try {
            if (table.getAttributes() != null && table.getAttributes().size() > 4000) {
                attributes = table.getAttributes();
                table.setAttributes(null);
            }
            put("updateTable", url, table);
            addTableAttributes(customerSpace, tableName, attributes);
        } catch(Exception e) {
            deleteTable(customerSpace, tableName);
            throw e;
        } finally {
            if (attributes != null) {
                table.setAttributes(attributes);
            }
        }
    }

    public Table cloneTable(String customerSpace, String tableName) {
        String url = constructUrl("/customerspaces/{customerSpace}/tables/{tableName}/clone", customerSpace, tableName);
        return post("cloneTable", url, null, Table.class);
    }

    public Table copyTable(String sourceTenantId, String tableName, String targetTenantId) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/tables/{tableName}/copy?targetcustomerspace={targetCustomerSpace}",
                sourceTenantId, tableName, targetTenantId);
        return post("copyTable", url, null, Table.class);
    }

    public List<String> getTableNames(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/tables", customerSpace);
        String[] importTableNames = get("getTables", url, String[].class);
        return Arrays.<String> asList(importTableNames);
    }

    public Table getTable(String customerSpace, String tableName) {
        String url = constructUrl("/customerspaces/{customerSpace}/tables/{tableName}", customerSpace, tableName);
        return get("getTable", url, Table.class);
    }

    @SuppressWarnings("unchecked")
    public List<ColumnMetadata> getTableColumns(String customerSpace, String tableName) {
        String url = constructUrl("/customerspaces/{customerSpace}/tables/{tableName}/columns", customerSpace, tableName);
        return JsonUtils.convertList(get("get table columns", url, List.class), ColumnMetadata.class);
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

    public Boolean createArtifact(String customerSpace, String moduleName, String artifactName, Artifact artifact) {
        String url = constructUrl("/customerspaces/{customerSpace}/modules/{moduleName}/artifacts/{artifactName}", //
                customerSpace, moduleName, artifactName);
        return post("createArtifact", url, artifact, Boolean.class);
    }

    public Module getModule(String customerSpace, String moduleName) {
        String url = constructUrl("/customerspaces/{customerSpace}/modules/{moduleName}", customerSpace, moduleName);
        return get("getModule", url, Module.class);
    }

    public void validateArtifact(String customerSpace, ArtifactType artifactType, String filePath) {
        String url = constructUrl("/customerspaces/{customerSpace}/artifacttype/{artifactType}?file={filePath}",
                customerSpace, artifactType, filePath);
        post("validateArtifact", url, null, ResponseDocument.class);
    }

    public Boolean createColumnResults(List<ColumnRuleResult> results) {
        String url = constructUrl("/ruleresults/column");
        return post("createColumnResults", url, results, Boolean.class);
    }

    public Boolean createRowResults(List<RowRuleResult> results) {
        String url = constructUrl("/ruleresults/row");
        return post("createRowResults", url, results, Boolean.class);
    }

    @SuppressWarnings("unchecked")
    public List<ColumnRuleResult> getColumnResults(String modelId) {
        String url = constructUrl("/ruleresults/column/{modelId}", modelId);
        return get("getColumnResults", url, List.class);
    }

    @SuppressWarnings("unchecked")
    public List<RowRuleResult> getRowResults(String modelId) {
        String url = constructUrl("/ruleresults/row/{modelId}", modelId);
        return get("getRowResults", url, List.class);
    }

    public ModelReviewData getReviewData(String customerSpace, String modelId, String eventTableName) {
        String url = constructUrl("/ruleresults/reviewdata/{customerSpace}/{modelId}/{eventTableName}", customerSpace,
                modelId, eventTableName);
        return get("getReviewData", url, ModelReviewData.class);
    }

    public Artifact getArtifactByPath(String customerSpace, String artifactPath) {
        String url = constructUrl("/customerspaces/{customerSpace}/artifactpath?file={artifactPath}", //
                customerSpace, artifactPath);
        return get("getArtifactByPath", url, Artifact.class);
    }

    public Boolean provisionImportTables(Tenant tenant) {
        String url = constructUrl("/admin/provision");
        return post("provisionImportTables", url, tenant, Boolean.class);
    }
}
