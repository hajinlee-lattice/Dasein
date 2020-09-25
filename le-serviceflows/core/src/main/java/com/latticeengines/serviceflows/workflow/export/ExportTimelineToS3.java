package com.latticeengines.serviceflows.workflow.export;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;

@Component("exportTimelineToS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportTimelineToS3 extends BaseImportExportS3<ImportExportS3StepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExportTimelineToS3.class);

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {

    }

    private void addTableDir(String tableName, List<ImportExportRequest> requests, Long paTs) {
        if (StringUtils.isBlank(tableName)) {
            return;
        }
        Table table = metadataProxy.getTable(customer, tableName);
        if (table == null) {
            log.warn("Can not find the table=" + tableName + " for tenant=" + customer);
            return;
        }
        ImportExportRequest request = ImportExportRequest.exportAtlasTable( //
                customer, table, //
                pathBuilder, s3Bucket, podId, //
                yarnConfiguration, //
                fileStatus -> {
                    long modifiedTime = fileStatus.getModificationTime();
                    String path = fileStatus.getPath().toString();
                    log.info("Export PA, PA start time=" + paTs + " file modification time=" + modifiedTime
                            + " file=" + path + " tenantId=" + customer);
                    return fileStatus.getModificationTime() > paTs;
                });
        if (request != null) {
            requests.add(request);
        }
    }
}
