package com.latticeengines.proxy.exposed.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.Module;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modelreview.ColumnRuleResult;
import com.latticeengines.domain.exposed.modelreview.ModelReviewData;
import com.latticeengines.domain.exposed.modelreview.RowRuleResult;
import com.latticeengines.network.exposed.metadata.ArtifactInterface;
import com.latticeengines.network.exposed.metadata.DataFeedInterface;
import com.latticeengines.network.exposed.metadata.DataFeedTaskInterface;
import com.latticeengines.network.exposed.metadata.MetadataInterface;
import com.latticeengines.network.exposed.metadata.ModuleInterface;
import com.latticeengines.network.exposed.metadata.RuleResultInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("metadataProxy")
public class MetadataProxy extends MicroserviceRestApiProxy implements MetadataInterface, ArtifactInterface,
        RuleResultInterface, ModuleInterface, DataFeedTaskInterface, DataFeedInterface {

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
    public MetadataSegment createOrUpdateSegment(String customerSpace, MetadataSegment metadataSegment) {
        String url = constructUrl("/customerspaces/{customerSpace}/segments", //
                customerSpace);
        return post("createOrUpdateSegment", url, metadataSegment, MetadataSegment.class);
    }

    @Override
    public void deleteSegmentByName(String customerSpace, String segmentName) {
        String url = constructUrl("/customerspaces/{customerSpace}/segments/{segmentName}", //
                customerSpace, segmentName);
        delete("deleteSegmentByName", url);
    }

    @Override
    public MetadataSegment getMetadataSegmentByName(String customerSpace, String segmentName) {
        String url = constructUrl("/customerspaces/{customerSpace}/segments/name/{segmentName}", //
                customerSpace, segmentName);
        return get("getSegment", url, MetadataSegment.class);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public List<MetadataSegment> getMetadataSegments(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/segments/all", customerSpace);
        List raw = get("getSegments", url, List.class);
        return JsonUtils.convertList(raw, MetadataSegment.class);
    }

    @Override
    public Boolean dataFeedTaskExist(String customerSpace, String dataFeedType, String entity) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeedtask/exist/{dataFeedType}/{entity}",
                customerSpace, dataFeedType, entity);
        return get("dataFeedTaskExist", url, Boolean.class);
    }

    @Override
    public void createDataFeedTask(String customerSpace, String dataFeedName, DataFeedTask dataFeedTask) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeedtask/{dataFeedName}/create",
                customerSpace, dataFeedName);
        post("createDataFeedTask", url, dataFeedTask, Void.class);
    }

    @Override
    public DataFeedTask getDataFeedTask(String customerSpace, String source, String dataFeedType, String entity,
            String dataFeedName) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datafeedtask/{source}/{dataFeedType}/{entity}/{dataFeedName}",
                customerSpace, source, dataFeedType, entity, dataFeedName);
        return get("getDataFeedTask", url, DataFeedTask.class);
    }

    @Override
    public DataFeedTask getDataFeedTask(String customerSpace, String id) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeedtask/{id}", customerSpace, id);
        return get("getDataFeedTaskById", url, DataFeedTask.class);
    }

    @Override
    public void updateDataFeedTask(String customerSpace, DataFeedTask dataFeedTask) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeedtask/update", customerSpace);
        post("updateDataFeedTask", url, dataFeedTask, Void.class);
    }

    @Override
    public void registerExtract(String customerSpace, String taskId, String tableName, Extract extract) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeedtask/registerextract/{taskId}/{tableName}",
                customerSpace, taskId, tableName);
        post("registerExtract", url, extract, Void.class);
    }

    @Override
    public DataFeedExecution startExecution(String customerSpace, String datafeedName) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeeds/{datafeedName}/startexecution",
                customerSpace, datafeedName);
        return post("startExecution", url, null, DataFeedExecution.class);
    }

    @Override
    public DataFeed findDataFeedByName(String customerSpace, String datafeedName) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeeds/{datafeedName}", customerSpace,
                datafeedName);
        return get("findDataFeedByName", url, DataFeed.class);
    }

    @Override
    public DataFeedExecution finishExecution(String customerSpace, String datafeedName) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeeds/{datafeedName}/finishexecution",
                customerSpace, datafeedName);
        return post("finishExecution", url, null, DataFeedExecution.class);
    }

    @Override
    public DataFeed createDataFeed(String customerSpace, DataFeed datafeed) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeeds", customerSpace);
        return post("createDataFeed", url, datafeed, DataFeed.class);
    }

    @Override
    public DataFeedExecution failExecution(String customerSpace, String datafeedName) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeeds/{datafeedName}/failexecution",
                customerSpace, datafeedName);
        return post("failExecution", url, null, DataFeedExecution.class);
    }

    @Override
    public DataFeedExecution updateExecutionWorkflowId(String customerSpace, String datafeedName, Long workflowId) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datafeeds/{datafeedName}/execution/workflow/{workflowId}",
                customerSpace, datafeedName, workflowId);
        return post("updateExecutionWorkflowId", url, null, DataFeedExecution.class);
    }

    @Override
    public void updateDataFeedStatus(String customerSpace, String datafeedName, Status status) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeeds/{datafeedName}/status/{status}",
                customerSpace, datafeedName, status);
        put("updateDataFeedStatus", url, null);
    }

}
