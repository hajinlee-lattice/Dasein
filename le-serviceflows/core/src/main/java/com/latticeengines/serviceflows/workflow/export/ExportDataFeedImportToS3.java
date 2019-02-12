package com.latticeengines.serviceflows.workflow.export;

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

import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.eai.EaiJobDetailProxy;

@Component("exportDataFeedImportToS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportDataFeedImportToS3 extends BaseImportExportS3<ImportExportS3StepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExportDataFeedImportToS3.class);

    @Inject
    private EaiJobDetailProxy eaiJobDetailProxy;

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {
        String applicationId = getOutputValue(WorkflowContextConstants.Outputs.EAI_JOB_APPLICATION_ID);
        if (applicationId == null) {
            log.warn("There's no application Id! tenentId=" + tenantId);
        }
        EaiImportJobDetail eaiImportJobDetail = eaiJobDetailProxy.getImportJobDetailByAppId(applicationId);
        if (eaiImportJobDetail == null) {
            log.warn(String.format("Cannot find the job detail for applicationId=%s, tenantId=%s", applicationId,
                    tenantId));
            return;
        }
        List<String> pathList = eaiImportJobDetail.getPathDetail();
        pathList = pathList.stream().filter(StringUtils::isNotBlank).collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(pathList)) {
            pathList.forEach(p -> {
                p = pathBuilder.getFullPath(p);
                String hdfsPrefix = "/Pods/" + podId;
                int index = p.indexOf(hdfsPrefix);
                if (index > 0)
                    p = p.substring(index);
                String tgtDir = pathBuilder.convertAtlasTableDir(p, podId, tenantId, s3Bucket);
                requests.add(new ImportExportRequest(p, tgtDir));
            });
            String filePath = getStringValueFromContext(WorkflowContextConstants.Outputs.EAI_JOB_INPUT_FILE_PATH);
            if (StringUtils.isNotBlank(filePath)) {
                String tgtDir = pathBuilder.convertAtlasFile(filePath, podId, tenantId, s3Bucket);
                requests.add(new ImportExportRequest(filePath, tgtDir));
            }
        } else {
            log.warn("There's no avro path found to export!");
        }

    }

}
