package com.latticeengines.dcp.workflow.steps.export;

import java.util.List;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.dcp.steps.DCPExportStepConfiguration;
import com.latticeengines.serviceflows.workflow.export.BaseImportExportS3;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;

@Component("exportSourceImportToS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportSourceImportToS3 extends BaseImportExportS3<DCPExportStepConfiguration> {

//    private static final Logger log = LoggerFactory.getLogger(ExportSourceImportToS3.class);
//
//    @Inject
//    private EaiJobDetailProxy eaiJobDetailProxy;
//
//    @Inject
//    private UploadProxy uploadProxy;

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {
//
//        Upload upload = uploadProxy.getUpload(customer, configuration.getUploadPid());
//        if (upload != null && upload.getUploadConfig() != null) {
//            String errorFilePath = getStringValueFromContext(IMPORT_ERROR_FILE);
//            if (StringUtils.isNotBlank(errorFilePath) && StringUtils.isNotBlank(upload.getUploadConfig().getUploadImportedErrorFilePath())) {
//                requests.add(new ImportExportRequest(errorFilePath, upload.getUploadConfig().getUploadImportedErrorFilePath()));
//            }
//        }
//        String applicationId = getOutputValue(WorkflowContextConstants.Outputs.EAI_JOB_APPLICATION_ID);
//        if (applicationId == null) {
//            log.warn("There's no application Id! tenentId=" + tenantId);
//        }
//        EaiImportJobDetail eaiImportJobDetail = eaiJobDetailProxy.getImportJobDetailByAppId(applicationId);
//        if (eaiImportJobDetail == null) {
//            log.warn(String.format("Cannot find the job detail for applicationId=%s, tenantId=%s", applicationId,
//                    tenantId));
//            return;
//        }
//        List<String> pathList = eaiImportJobDetail.getPathDetail();
//        if (CollectionUtils.isEmpty(pathList) || pathList.size() != 1) {
//            log.error("There should be exact one data path to export, but found " + CollectionUtils.size(pathList));
//        } else {
//            String path = pathList.get(0);
//            path = pathBuilder.getFullPath(path);
//            String hdfsPrefix = "/Pods/" + podId;
//            int index = path.indexOf(hdfsPrefix);
//            if (index > 0)
//                path = path.substring(index);
//            String tgtDir = pathBuilder.convertAtlasTableDir(path, podId, tenantId, s3Bucket);
//            requests.add(new ImportExportRequest(path, tgtDir));
//        }
    }
}
