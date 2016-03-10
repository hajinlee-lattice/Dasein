package com.latticeengines.pls.workflow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.leadprioritization.workflow.PMMLModelWorkflowConfiguration;
import com.latticeengines.pls.service.MetadataFileUploadService;

@Component("pmmlModelWorkflowSubmitter")
public class PMMLModelWorkflowSubmitter extends BaseModelWorkflowSubmitter {
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(PMMLModelWorkflowSubmitter.class);
    
    @Autowired
    private MetadataFileUploadService metadataFileUploadService;

    public ApplicationId submit(String modelName, String moduleName, String pmmlArtifactName, String pivotArtifactName) {
        Map<String, Artifact> pmmlArtifacts = getArtifactMap(
                metadataFileUploadService.getArtifacts(moduleName, ArtifactType.PMML));
        Map<String, Artifact> pivotArtifacts = getArtifactMap(
                metadataFileUploadService.getArtifacts(moduleName, ArtifactType.PivotMapping));
        
        Artifact pmmlArtifact = pmmlArtifacts.get(pmmlArtifactName);
        Artifact pivotArtifact = pivotArtifacts.get(pivotArtifactName);
        
        if (pmmlArtifact == null) {
            throw new LedpException(LedpCode.LEDP_28020, new String[] { pmmlArtifactName });
        }
        
        if (pivotArtifact == null) {
            throw new LedpException(LedpCode.LEDP_28020, new String[] { pivotArtifactName });
        }
        
        PMMLModelWorkflowConfiguration configuration = new PMMLModelWorkflowConfiguration.Builder()
                .podId(CamilleEnvironment.getPodId()) //
                .microServiceHostPort(microserviceHostPort) //
                .customer(getCustomerSpace()) //
                .workflow("pmmlModelWorkflow") //
                .modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                .modelName(modelName) //
                .pmmlArtifactPath(pmmlArtifact.getPath()) //
                .pivotArtifactPath(pivotArtifact.getPath()) //
                .build();
        AppSubmission submission = workflowProxy.submitWorkflowExecution(configuration);
        String applicationId = submission.getApplicationIds().get(0);
        return YarnUtils.appIdFromString(applicationId);
    }
    
    private Map<String, Artifact> getArtifactMap(List<Artifact> artifacts) {
        Map<String, Artifact> map = new HashMap<>();
        for (Artifact artifact : artifacts) {
            map.put(artifact.getName(), artifact);
        }
        return map;
    }

}
