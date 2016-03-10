package com.latticeengines.leadprioritization.workflow.steps;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.serviceflows.workflow.core.ModelingServiceExecutor;

@Component("createPMMLModel")
public class CreatePMMLModel extends BaseWorkflowStep<CreatePMMLModelConfiguration> {

    private static final Log log = LogFactory.getLog(CreatePMMLModel.class);

    @Override
    public void execute() {
        log.info("Inside CreatePMMLModel execute()");
        ModelingServiceExecutor executor = new ModelingServiceExecutor(createModelingServiceExecutorBuilder(configuration));
        try {
            executor.modelForPMML();
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_28019, new String[] { configuration.getPmmlArtifactPath() });
        }
    }

    protected ModelingServiceExecutor.Builder createModelingServiceExecutorBuilder(
            CreatePMMLModelConfiguration modelStepConfiguration) {
        String metadataContents = getMetadataContents();

        ModelingServiceExecutor.Builder bldr = new ModelingServiceExecutor.Builder();
        bldr.sampleSubmissionUrl("/modeling/samples") //
                .profileSubmissionUrl("/modeling/profiles") //
                .modelSubmissionUrl("/modeling/models") //
                .retrieveFeaturesUrl("/modeling/features") //
                .retrieveJobStatusUrl("/modeling/jobs/%s") //
                .retrieveModelingJobStatusUrl("/modeling/modelingjobs/%s") //
                .modelingServiceHostPort(modelStepConfiguration.getMicroServiceHostPort()) //
                .modelingServiceHdfsBaseDir(modelStepConfiguration.getModelingServiceHdfsBaseDir()) //
                .customer(modelStepConfiguration.getCustomerSpace().toString()) //
                .metadataContents(metadataContents) //
                .yarnConfiguration(yarnConfiguration) //
                .productType(modelStepConfiguration.getProductType());

        return bldr;
    }
    
    private String getMetadataContents() {
        String pivotArtifactPath = configuration.getPivotArtifactPath();
        return null;
    }

}
