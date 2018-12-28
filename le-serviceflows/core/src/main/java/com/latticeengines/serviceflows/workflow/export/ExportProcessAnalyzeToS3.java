package com.latticeengines.serviceflows.workflow.export;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;

@Component("exportProcessAnalyzeToS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportProcessAnalyzeToS3 extends BaseImportExportS3<ImportExportS3StepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExportProcessAnalyzeToS3.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {

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

    private void addMatchError(List<BaseImportExportS3<ImportExportS3StepConfiguration>.ImportExportRequest> requests) {
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
        List<Extract> extracts = table.getExtracts();
        if (CollectionUtils.isEmpty(extracts) || StringUtils.isBlank(extracts.get(0).getPath())) {
            log.warn("Can not find extracts of the table=" + tableName + " for tenant=" + customer);
            return;
        }
        try {
            String srcDir = pathBuilder.getFullPath(extracts.get(0).getPath());
            FileStatus fileStatus = HdfsUtils.getFileStatus(yarnConfiguration, srcDir);
            log.info("Export PA, PA start time=" + paTs + " file modification time=" + fileStatus.getModificationTime()
                    + " file=" + srcDir + " tenantId=" + customer);
            if (fileStatus.getModificationTime() > paTs) {
                String tgtDir = pathBuilder.convertAtlasTableDir(srcDir, podId, tenantId, s3Bucket);
                requests.add(new ImportExportRequest(srcDir, tgtDir, tableName, true));
            }
        } catch (Exception ex) {
            log.warn("Can not get time stamp of table=" + tableName + " for tenant=" + customer + " error="
                    + ex.getMessage());
        }
    }

}
