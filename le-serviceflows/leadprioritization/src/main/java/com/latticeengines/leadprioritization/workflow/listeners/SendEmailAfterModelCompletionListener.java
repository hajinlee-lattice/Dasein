package com.latticeengines.leadprioritization.workflow.listeners;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.AdditionalEmailInfo;
import com.latticeengines.serviceflows.workflow.modeling.ModelStepConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;
import com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("sendEmailAfterModelCompletionListener")
public class SendEmailAfterModelCompletionListener extends LEJobListener {

    private static final Log log = LogFactory.getLog(SendEmailAfterModelCompletionListener.class);

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        String tenantId = jobExecution.getJobParameters().getString("CustomerSpace");
        log.info("tenantid: " + tenantId);
        String hostPort = jobExecution.getJobParameters().getString("Internal_Resource_Host_Port");
        log.info("hostPort: " + hostPort);
        String userId = jobExecution.getJobParameters().getString("User_Id");
        AdditionalEmailInfo emailInfo = new AdditionalEmailInfo();
        emailInfo.setUserId(userId);
        String modelStepConfiguration = jobExecution.getJobParameters().getString(
                ModelStepConfiguration.class.getCanonicalName());
        ModelStepConfiguration config = JsonUtils.deserialize(modelStepConfiguration, ModelStepConfiguration.class);
        if (config != null) {
            String modelName = config.getModelName();
            emailInfo.setModelId(modelName);
            log.info(String.format("userId: %s; modelName: %s", emailInfo.getUserId(), emailInfo.getModelId()));
            InternalResourceRestApiProxy proxy = new InternalResourceRestApiProxy(hostPort);
            try {
                proxy.sendPlsCreateModelEmail(jobExecution.getStatus().name(), tenantId, emailInfo);
            } catch (Exception e) {
                log.error("Can not send create model email: " + e.getMessage());
            }
        }
    }

}
