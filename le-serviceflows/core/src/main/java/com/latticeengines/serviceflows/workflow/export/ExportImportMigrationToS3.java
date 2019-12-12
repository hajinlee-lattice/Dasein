package com.latticeengines.serviceflows.workflow.export;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.ImportMigrateReport;
import com.latticeengines.domain.exposed.cdl.ImportMigrateTracking;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.cdl.MigrateTrackingProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;

@Component("exportImportMigrationToS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportImportMigrationToS3 extends BaseImportExportS3<ImportExportS3StepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExportImportMigrationToS3.class);

    @Inject
    private MigrateTrackingProxy migrateTrackingProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {
        Long migrateTrackingPid =
                Long.parseLong(getOutputValue(WorkflowContextConstants.Outputs.IMPORT_MIGRATE_TRACKING_ID));
        ImportMigrateTracking importMigrateTracking = migrateTrackingProxy.getMigrateTracking(customer,
                migrateTrackingPid);
        if (importMigrateTracking == null || importMigrateTracking.getReport() == null) {
            log.warn("Nothing to export for tenant {}, skip export s3 step", customer);
            return;
        }
        ImportMigrateReport migrateReport = importMigrateTracking.getReport();
        List<String> pathList = new ArrayList<>();
        pathList.addAll(getExtractPathList(migrateReport.getAccountDataTables())
                .stream().filter(StringUtils::isNotBlank).distinct().collect(Collectors.toList()));
        pathList.addAll(getExtractPathList(migrateReport.getContactDataTables())
                .stream().filter(StringUtils::isNotBlank).distinct().collect(Collectors.toList()));
        pathList.addAll(getExtractPathList(migrateReport.getTransactionDataTables())
                .stream().filter(StringUtils::isNotBlank).distinct().collect(Collectors.toList()));
        if (CollectionUtils.isNotEmpty(pathList)) {
            pathList.forEach(p -> {
                p = pathBuilder.getFullPath(p);
                String hdfsPrefix = "/Pods/" + podId;
                int index = p.indexOf(hdfsPrefix);
                if (index > 0) {
                    p = p.substring(index);
                }
                String tgtDir = pathBuilder.convertAtlasTableDir(p, podId, tenantId, s3Bucket);
                requests.add(new ImportExportRequest(p, tgtDir));
            });
        } else {
            log.warn("There's no migrated avro path found to export!");
        }
    }

    private List<String> getExtractPathList(List<String> dataTableNames) {
        List<String> extractPathList = new ArrayList<>();
        if (CollectionUtils.isEmpty(dataTableNames)) {
            return extractPathList;
        }
        dataTableNames.forEach(tableName -> {
            Table table = metadataProxy.getTable(customer, tableName);
            if (table != null && CollectionUtils.isNotEmpty(table.getExtracts())) {
                extractPathList.addAll(table.getExtracts().stream().map(Extract::getPath).collect(Collectors.toList()));
            }
        });
        return extractPathList;
    }
}
