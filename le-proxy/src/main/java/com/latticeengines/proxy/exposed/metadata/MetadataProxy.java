package com.latticeengines.proxy.exposed.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.PostConstruct;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Module;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.modelreview.ColumnRuleResult;
import com.latticeengines.domain.exposed.modelreview.ModelReviewData;
import com.latticeengines.domain.exposed.modelreview.RowRuleResult;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("metadataProxy")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
@CacheConfig(cacheNames = CacheName.Constants.MetadataCacheName)
public class MetadataProxy extends MicroserviceRestApiProxy {

    private static final Logger log = LoggerFactory.getLogger(MetadataProxy.class);

    private static final Integer ATTRIBUTE_BATCH_SIZE = 5000;

    @PostConstruct
    public void init() {

    }

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
            if (table.getAttributes() != null && table.getAttributes().size() > ATTRIBUTE_BATCH_SIZE) {
                log.info("CreateTable request for table: {} - Attributes: {} ", tableName, table.getAttributes().size());
                attributes = table.getAttributes();
                table.setAttributes(Collections.emptyList());
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
        for (int i = 0; (i * ATTRIBUTE_BATCH_SIZE) < attributes.size(); i++) {
            List<Attribute> subList = attributes.subList(i * ATTRIBUTE_BATCH_SIZE, Math.min(i * ATTRIBUTE_BATCH_SIZE + ATTRIBUTE_BATCH_SIZE, attributes.size()));
            post("addTableAttributes", url, subList, null);
        }
    }

    public Boolean resetTables(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/tables/reset", customerSpace);
        return post("reset", url, null, Boolean.class);
    }

    public void renameTable(String customerSpace, String tableName, String newTableName) {
        String url = constructUrl("/customerspaces/{customerSpace}/tables/{tableName}/rename/{newTableName}", customerSpace, tableName, newTableName);
        try {
            post("renameTable", url, null);
        } catch(Exception e) {
            throw e;
        }
    }

    public void updateTable(String customerSpace, String tableName, Table table) {
        String url = constructUrl("/customerspaces/{customerSpace}/tables/{tableName}", customerSpace, tableName);
        List<Attribute> attributes = null;
        try {
            // This is to take care of backward compatibility changes for rename usecase.
            if (!tableName.equals(table.getName())) {
                log.info("Performing renameTable op from {} - to  {}", table.getName(), tableName);
                renameTable(customerSpace, table.getName(), tableName);
                table.setName(tableName);
            }
            //- End of rename usecase
            if (table.getAttributes() != null && table.getAttributes().size() > ATTRIBUTE_BATCH_SIZE) {
                log.info("UpdateTable request for table: {} - Attributes: {} ", tableName, table.getAttributes().size());
                attributes = table.getAttributes();
                table.setAttributes(Collections.emptyList());
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
        if (StringUtils.isEmpty(tableName)) {
            return null;
        }
        long columnCount = getTableAttributeCount(customerSpace, tableName);
        if (columnCount == 0) {
            return null;
        } else if (ATTRIBUTE_BATCH_SIZE > columnCount) {
            String url = constructUrl("/customerspaces/{customerSpace}/tables/{tableName}", customerSpace, tableName);
            return get("getTable", url, Table.class);
        }

        // Need to split the attributes into Chunks
        Table table = getTableSummary(customerSpace, tableName);
        table.setAttributes(getTableAttributes(customerSpace, tableName, columnCount));
        return table;
    }

    public boolean dataTableExists(String customerSpace, String tableName) {
        return getTableSummary(customerSpace, tableName) != null;
    }


    public long getTableAttributeCount(String customerSpace, String tableName) {
        String url = constructUrl("/customerspaces/{customerSpace}/tables/{tableName}/attribute_count", customerSpace, tableName);
        Long count = get("getTableColumnCount", url, Long.class);
        log.info("GetTableAttributeCount for {}-{} , Count: {}", customerSpace, tableName, count);
        if (count == null) {
            new Thread(() -> deleteTable(customerSpace, tableName)).start();
        }
        return (count == null) ? 0 : count;
    }

    /**
     * @param customerSpace
     * @param tableName
     * @return Table entity without Attributes collection
     */
    public Table getTableSummary(String customerSpace, String tableName) {
        String url = constructUrl("/customerspaces/{customerSpace}/tables/{tableName}?include_attributes=false", customerSpace, tableName);
        return get("getTableSummary", url, Table.class);
    }

    public String getAvroDir(String customerSpace, String tableName) {
        Table table = getTable(customerSpace, tableName);
        if (table == null) {
            throw new IllegalArgumentException("Cannot find table named " + tableName);
        }
        List<Extract> extracts = table.getExtracts();
        if (CollectionUtils.isEmpty(extracts) || extracts.size() != 1) {
            throw new IllegalArgumentException("Table " + tableName + " does not have single extract");
        }
        Extract extract = extracts.get(0);
        String path = extract.getPath();
        if (path.endsWith(".avro") || path.endsWith("/")) {
            path = path.substring(0, path.lastIndexOf("/"));
        }
        return path;
    }

    public List<ColumnMetadata> getTableColumns(String customerSpace, String tableName) {
        // This returns all table columns
        List<Attribute> attributes = getTableAttributes(customerSpace, tableName, null);
        if (attributes == null) {
            return Collections.emptyList();
        }
        return attributes.stream().parallel().map(Attribute::getColumnMetadata).collect(Collectors.toList());
    }

    public List<Attribute> getTableAttributes(String customerSpace, String tableName, Long columnCount) {
        long attributeCount = columnCount == null ? getTableAttributeCount(customerSpace, tableName) : columnCount;
        List<Attribute> attributeLst = Collections.synchronizedList(new ArrayList<>());

        log.info("Getting Table: {} with Attributes: {} in chunks of {} ", tableName, attributeCount, ATTRIBUTE_BATCH_SIZE);

        IntStream.range(0, (int) (Math.ceil((double)attributeCount/ATTRIBUTE_BATCH_SIZE))).forEach(page -> {
            List<Attribute> attributePage = getTableAttributes(customerSpace, tableName, page+1, ATTRIBUTE_BATCH_SIZE);
            attributeLst.addAll(attributePage);
        });
        return attributeLst;
    }

    public List<Attribute> getTableAttributes(String customerSpace, String tableName, int page, long size) {
        String url = constructUrl("/customerspaces/{customerSpace}/tables/{tableName}/attributes", customerSpace, tableName);
        url = String.format("%s?page=%d&size=%d", url, page, size);
        return JsonUtils.convertList(get("get table columns", url, List.class), Attribute.class);
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
