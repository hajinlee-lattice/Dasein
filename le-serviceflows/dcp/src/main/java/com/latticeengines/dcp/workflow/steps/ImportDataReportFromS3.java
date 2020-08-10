package com.latticeengines.dcp.workflow.steps;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dcp.DataReportMode;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.dcp.DunsCountCache;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.DCPReportImportConfiguration;
import com.latticeengines.proxy.exposed.dcp.DataReportProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.export.BaseImportExportS3;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;

import io.micrometer.core.instrument.util.StringUtils;

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

    private void addTableToList(String customerSpace, String ownerId, DataReportRecord.Level level,
                                List<Table> tables) {
        DunsCountCache cache = dataReportProxy.getDunsCount(customerSpace, level, ownerId);
        if (StringUtils.isNotBlank(cache.getDunsCountTableName())) {
            Table metadataTable = metadataProxy.getTable(customerSpace, cache.getDunsCountTableName());
            if (metadataTable != null) {
                tables.add(metadataTable);
            }
        }
    }

    private List<Table> getImportTables(String rootId, DataReportRecord.Level level, DataReportMode mode) {
        List<Table> tables = new ArrayList<>();
        String customerSpace = configuration.getCustomerSpace().toString();
        switch (level) {
            case Tenant:
                Set<String> projectIds = dataReportProxy.getChildrenIds(customerSpace, level, rootId);
                projectIds.forEach(projectId -> {
                    if (DataReportMode.UPDATE.equals(mode)) {
                        addTableToList(customerSpace, projectId, DataReportRecord.Level.Project, tables);
                    }
                    Set<String> sourceIds = dataReportProxy.getChildrenIds(customerSpace,
                            DataReportRecord.Level.Project, projectId);
                    sourceIds.forEach(sourceId -> {
                        if (DataReportMode.UPDATE.equals(mode)) {
                            addTableToList(customerSpace, sourceId, DataReportRecord.Level.Source, tables);
                        }
                        Set<String> retrievedUploadIds = dataReportProxy.getChildrenIds(customerSpace,
                                DataReportRecord.Level.Source, sourceId);
                        retrievedUploadIds.forEach(uploadId ->
                            addTableToList(customerSpace, uploadId, DataReportRecord.Level.Upload, tables)
                        );
                    });
                });
                break;
            case Project:
                Set<String> sourceIds = dataReportProxy.getChildrenIds(customerSpace, level, rootId);
                sourceIds.forEach(sourceId -> {
                    if (DataReportMode.UPDATE.equals(mode)) {
                        addTableToList(customerSpace, sourceId, DataReportRecord.Level.Source, tables);
                    }
                    Set<String> retrievedUploadIds = dataReportProxy.getChildrenIds(customerSpace,
                            DataReportRecord.Level.Source, sourceId);
                    retrievedUploadIds.forEach(uploadId -> {
                            addTableToList(customerSpace, uploadId, DataReportRecord.Level.Upload, tables);
                    });
                });
                break;
            case Source:
                Set<String> retrievedUploadIds = dataReportProxy.getChildrenIds(customerSpace.toString(), level,
                        rootId);
                retrievedUploadIds.forEach(uploadId -> {
                        addTableToList(customerSpace, uploadId, DataReportRecord.Level.Upload, tables);
                });
                break;
            default:
                break;
        }
        return tables;
    }
}
