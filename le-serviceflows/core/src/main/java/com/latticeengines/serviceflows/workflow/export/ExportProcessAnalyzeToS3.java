package com.latticeengines.serviceflows.workflow.export;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;

@Component("exportProcessAnalyzeToS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportProcessAnalyzeToS3 extends BaseImportExportS3<ImportExportS3StepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExportProcessAnalyzeToS3.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {
        boolean shouldSkip = getObjectFromContext(SKIP_PUBLISH_PA_TO_S3, Boolean.class);
        if (!shouldSkip) {
            DataCollection.Version inactiveVersion = getObjectFromContext(CDL_INACTIVE_VERSION,
                    DataCollection.Version.class);
            Long paTs = getLongValueFromContext(PA_TIMESTAMP);
            for (TableRoleInCollection role : TableRoleInCollection.values()) {
                List<String> inactiveTableNames = dataCollectionProxy.getTableNames(customer, role, inactiveVersion);
                if (CollectionUtils.isNotEmpty(inactiveTableNames)) {
                    log.info("Start to add inactive tables for tenant=" + customer + " role=" + role.name());
                    inactiveTableNames.forEach(t -> {
                        addTableDir(t, requests, paTs);
                    });
                }
            }
            addMatchError(requests);
        }
    }

    private void addMatchError(List<ImportExportRequest> requests) {
        String errorFile = getStringValueFromContext(WorkflowContextConstants.Outputs.POST_MATCH_ERROR_EXPORT_PATH);
        if (StringUtils.isNotBlank(errorFile)) {
            String tgtPath = pathBuilder.exploreS3FilePath(errorFile, s3Bucket);
            requests.add(new ImportExportRequest(errorFile, tgtPath));
        }
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
                yarnConfiguration, null, //
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
