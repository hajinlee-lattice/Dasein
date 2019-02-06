package com.latticeengines.pls.workflow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.serviceflows.modeling.PMMLModelWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.pls.service.MetadataFileUploadService;

@Component("pmmlModelWorkflowSubmitter")
public class PMMLModelWorkflowSubmitter extends BaseModelWorkflowSubmitter {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(PMMLModelWorkflowSubmitter.class);

    @Autowired
    private MetadataFileUploadService metadataFileUploadService;

    @Value("${pls.modeling.workflow.mem.mb}")
    protected int workflowMemMb;

    public ApplicationId submit(String modelName, String modelDisplayName, String moduleName, String pivotFileName,
            String pmmlFileName, SchemaInterpretation schemaInterpretation) {
        Map<String, Artifact> pmmlArtifacts = getArtifactMap(
                metadataFileUploadService.getArtifacts(moduleName, ArtifactType.PMML));
        Map<String, Artifact> pivotArtifacts = getArtifactMap(
                metadataFileUploadService.getArtifacts(moduleName, ArtifactType.PivotMapping));

        if (pmmlArtifacts.size() == 0) {
            throw new LedpException(LedpCode.LEDP_28020, new String[] { moduleName });
        }

        Artifact pmmlArtifact = pmmlArtifacts.get(pmmlFileName);
        Artifact pivotArtifact = pivotArtifacts.get(pivotFileName);

        if (pmmlArtifact == null) {
            throw new LedpException(LedpCode.LEDP_28025, new String[] { pmmlFileName, moduleName });
        }

        if (pivotFileName != null && pivotArtifact == null) {
            throw new LedpException(LedpCode.LEDP_28026, new String[] { pivotFileName, moduleName });
        }

        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "pmmlModelWorkflow");
        inputProperties.put(WorkflowContextConstants.Inputs.MODEL_DISPLAY_NAME, modelDisplayName);
        inputProperties.put(WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME, pmmlFileName);

        PMMLModelWorkflowConfiguration configuration = new PMMLModelWorkflowConfiguration.Builder()
                .podId(CamilleEnvironment.getPodId()) //
                .microServiceHostPort(microserviceHostPort) //
                .customer(getCustomerSpace()) //
                .modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                .modelName(modelName) //
                .displayName(modelDisplayName) //
                .pmmlArtifactPath(pmmlArtifact.getPath()) //
                .pivotArtifactPath(pivotArtifact != null ? pivotArtifact.getPath() : null) //
                .moduleName(moduleName != null ? moduleName : null) //
                .inputProperties(inputProperties) //
                .internalResourceHostPort(internalResourceHostPort) //
                .sourceSchemaInterpretation(schemaInterpretation.name()) //
                .workflowContainerMem(workflowMemMb) //
                .build();
        return workflowJobService.submit(configuration);
    }

    private Map<String, Artifact> getArtifactMap(List<Artifact> artifacts) {
        Map<String, Artifact> map = new HashMap<>();
        for (Artifact artifact : artifacts) {
            map.put(artifact.getName(), artifact);
        }
        return map;
    }

}
