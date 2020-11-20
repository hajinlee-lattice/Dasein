package com.latticeengines.cdl.workflow.steps.importdata;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.S3DataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.ImportListSegmentWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ExportListSegmentCSVToS3Configuration;
import com.latticeengines.serviceflows.workflow.export.BaseImportExportS3;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;

@Component("exportListSegmentCSVToS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportListSegmentCSVToS3
        extends BaseImportExportS3<ExportListSegmentCSVToS3Configuration> {

    private static final Logger log = LoggerFactory.getLogger(ExportListSegmentCSVToS3.class);

    @Override
    public void buildRequests(List<ImportExportRequest> requests) {
        uploadS3DataUnits(getObjectFromContext(ImportListSegmentWorkflowConfiguration.ACCOUNT_DATA_UNIT_NAME, String.class), requests);
        uploadS3DataUnits(getObjectFromContext(ImportListSegmentWorkflowConfiguration.CONTACT_DATA_UNIT_NAME, String.class), requests);
    }

    private void uploadS3DataUnits(String dataUnitName, List<ImportExportRequest> requests) {
        if (StringUtils.isNotEmpty(dataUnitName)) {
            String tenantId = configuration.getCustomerSpace().getTenantId();
            S3DataUnit s3DataUnit = (S3DataUnit) dataUnitProxy.getByNameAndType(tenantId, dataUnitName, DataUnit.StorageType.S3);
            if (s3DataUnit != null) {
                String path = s3DataUnit.getLinkedHdfsPath();
                if (StringUtils.isNotEmpty(path)) {
                    String srcDir = pathBuilder.getFullPath(path);
                    String tgtDir = pathBuilder.getS3AtlasDataUnitPrefix(s3Bucket, tenantId, s3DataUnit.getDataTemplateId(), dataUnitName);
                    log.info("Source Hdfs Directory: " + srcDir);
                    log.info("Target s3 Directory: " + tgtDir);
                    requests.add(new ImportExportRequest(srcDir, tgtDir, dataUnitName, true, false));
                }
            } else {
                throw new RuntimeException(String.format("Can't find s3 data unit by name {}.", dataUnitName));
            }
        }
    }
}
