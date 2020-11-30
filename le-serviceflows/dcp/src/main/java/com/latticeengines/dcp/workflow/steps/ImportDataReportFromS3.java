package com.latticeengines.dcp.workflow.steps;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.dcp.DataReportMode;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.dcp.DunsCountCache;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.DCPReportImportConfiguration;
import com.latticeengines.proxy.exposed.dcp.DataReportProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.export.BaseImportExportS3;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;


@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ImportDataReportFromS3  extends BaseImportExportS3<DCPReportImportConfiguration> {

    @Inject
    private DataReportProxy dataReportProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {
        String rootId = configuration.getRootId();
        DataReportRecord.Level level = configuration.getLevel();
        DataReportMode mode = configuration.getMode();

        List<Table> tables = getImportTables(rootId, level, mode);
        // super class will judge import or not
        tables.forEach(table -> addTableToRequestForImport(table, requests));
    }

    @Override
    protected ExecutorService getExporterPool(List<ImportExportRequest> requests) {
        int threadPoolSize = Math.min(16, requests.size());
        return ThreadPoolUtils.getFixedSizeThreadPool("s3-import-export", threadPoolSize);
    }

    /**
     *
     * @param customerSpace
     * @param ownerId
     * @param level
     * @param tables
     * @param tableNames
     * tableNames is used to deduplocate, in data report logic, there exists
     * parental report has the same duns count name with child's report
     */
    private void addTableToList(String customerSpace, String ownerId, DataReportRecord.Level level,
                                List<Table> tables, Set<String> tableNames) {
        DunsCountCache cache = dataReportProxy.getDunsCount(customerSpace, level, ownerId);
        String tableName = cache.getDunsCountTableName();
        if (StringUtils.isNotBlank(tableName) && !tableNames.contains(tableName)) {
            Table metadataTable = metadataProxy.getTable(customerSpace, tableName);
            if (metadataTable != null) {
                tables.add(metadataTable);
                tableNames.add(tableName);
            }
        }
    }

    private List<Table> getImportTables(String rootId, DataReportRecord.Level level, DataReportMode mode) {
        List<Table> tables = new ArrayList<>();
        Set<String> tableNames = new HashSet<>();
        String customerSpace = configuration.getCustomerSpace().toString();
        switch (level) {
            case Tenant:
                Set<String> projectIds = dataReportProxy.getChildrenIds(customerSpace, level, rootId);
                projectIds.forEach(projectId -> {
                    if (DataReportMode.UPDATE.equals(mode)) {
                        addTableToList(customerSpace, projectId, DataReportRecord.Level.Project, tables, tableNames);
                    }
                    Set<String> sourceIds = dataReportProxy.getChildrenIds(customerSpace,
                            DataReportRecord.Level.Project, projectId);
                    sourceIds.forEach(sourceId -> {
                        if (DataReportMode.UPDATE.equals(mode)) {
                            addTableToList(customerSpace, sourceId, DataReportRecord.Level.Source, tables, tableNames);
                        }
                        Set<String> retrievedUploadIds = dataReportProxy.getChildrenIds(customerSpace,
                                DataReportRecord.Level.Source, sourceId);
                        retrievedUploadIds.forEach(uploadId ->
                            addTableToList(customerSpace, uploadId, DataReportRecord.Level.Upload, tables, tableNames)
                        );
                    });
                });
                break;
            case Project:
                Set<String> sourceIds = dataReportProxy.getChildrenIds(customerSpace, level, rootId);
                sourceIds.forEach(sourceId -> {
                    if (DataReportMode.UPDATE.equals(mode)) {
                        addTableToList(customerSpace, sourceId, DataReportRecord.Level.Source, tables, tableNames);
                    }
                    Set<String> retrievedUploadIds = dataReportProxy.getChildrenIds(customerSpace,
                            DataReportRecord.Level.Source, sourceId);
                    retrievedUploadIds.forEach(uploadId -> {
                            addTableToList(customerSpace, uploadId, DataReportRecord.Level.Upload, tables, tableNames);
                    });
                });
                break;
            case Source:
                Set<String> retrievedUploadIds = dataReportProxy.getChildrenIds(customerSpace, level,
                        rootId);
                retrievedUploadIds.forEach(uploadId -> {
                        addTableToList(customerSpace, uploadId, DataReportRecord.Level.Upload, tables, tableNames);
                });
                break;
            default:
                break;
        }
        return tables;
    }
}
