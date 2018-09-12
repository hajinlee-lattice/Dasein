package com.latticeengines.serviceflows.workflow.export;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportToS3StepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.eai.EaiJobDetailProxy;

@Component("exportDataFeedImportToS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportDataFeedImportToS3 extends BaseExportToS3<ExportToS3StepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExportDataFeedImportToS3.class);

    @Inject
    private EaiJobDetailProxy eaiJobDetailProxy;

    @Override
    protected void buildRequests(List<ExportRequest> requests) {
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
        pathList.forEach(p -> {
            p = pathBuilder.getFullPath(p);
            String hdfsPrefix = "/Pods/" + podId;
            int index = p.indexOf(hdfsPrefix);
            if (index > 0)
                p = p.substring(index);
            String tgtDir = pathBuilder.convertAtlasTableDir(p, podId, tenantId, s3Bucket);
            requests.add(new ExportRequest(p, tgtDir));
        });

    }

}
