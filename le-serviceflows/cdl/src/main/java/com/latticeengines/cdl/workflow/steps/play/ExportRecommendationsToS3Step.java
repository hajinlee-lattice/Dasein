package com.latticeengines.cdl.workflow.steps.play;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.ExportRecommendationsToS3StepConfiguration;
import com.latticeengines.serviceflows.workflow.export.BaseImportExportS3;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;

@Component("exportRecommendationsToS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportRecommendationsToS3Step extends BaseImportExportS3<ExportRecommendationsToS3StepConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(ExportRecommendationsToS3Step.class);

    @Override
    public void buildRequests(List<ImportExportRequest> requests) {
        addTableDirs(getObjectFromContext(ADDED_RECOMMENDATION_TABLE, String.class), requests);
        addTableDirs(getObjectFromContext(DELETED_RECOMMENDATION_TABLE, String.class), requests);
    }

    private void addTableDirs(String tableName, List<ImportExportRequest> requests) {
        if (StringUtils.isNotEmpty(tableName)) {
            Table table = metadataProxy.getTable(configuration.getCustomerSpace().getTenantId(), tableName);
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
}
