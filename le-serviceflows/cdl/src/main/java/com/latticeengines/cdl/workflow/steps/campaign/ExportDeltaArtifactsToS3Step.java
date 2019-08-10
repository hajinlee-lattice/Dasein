package com.latticeengines.cdl.workflow.steps.campaign;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.ExportDeltaArtifactsToS3StepConfiguration;
import com.latticeengines.serviceflows.workflow.export.BaseImportExportS3;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;

public class ExportDeltaArtifactsToS3Step extends BaseImportExportS3<ExportDeltaArtifactsToS3StepConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(ExportDeltaArtifactsToS3Step.class);

    @Override
    public void buildRequests(List<ImportExportRequest> requests) {
        addTableDirs(getObjectFromContext(ADDED_ACCOUNTS_DELTA_TABLE, Table.class), requests);
        addTableDirs(getObjectFromContext(REMOVED_ACCOUNTS_DELTA_TABLE, Table.class), requests);
        addTableDirs(getObjectFromContext(ADDED_CONTACTS_DELTA_TABLE, Table.class), requests);
        addTableDirs(getObjectFromContext(REMOVED_CONTACTS_DELTA_TABLE, Table.class), requests);
        addTableDirs(getObjectFromContext(FULL_ACCOUNT_TABLE_NAME, Table.class), requests);
        addTableDirs(getObjectFromContext(FULL_CONTACTS_UNIVERSE, Table.class), requests);
    }

    private void addTableDirs(Table table, List<ImportExportRequest> requests) {
        if (table != null) {
            List<Extract> extracts = table.getExtracts();
            if (CollectionUtils.isNotEmpty(extracts)) {
                extracts.forEach(extract -> {
                    if (StringUtils.isNotBlank(extract.getPath())) {
                        String srcDir = pathBuilder.getFullPath(extract.getPath());
                        String tgtDir = pathBuilder.convertAtlasTableDir(srcDir, podId, tenantId, s3Bucket);
                        log.info("Source HDFS Directory: " + srcDir);
                        log.info("Target S3 Directory: " + tgtDir);
                        requests.add(new ImportExportRequest(srcDir, tgtDir, table.getName(), true, false));
                    }
                });
            } else {
                log.warn("Can not find the table=" + table + " for tenant=" + customer);
            }
        }
    }
}
