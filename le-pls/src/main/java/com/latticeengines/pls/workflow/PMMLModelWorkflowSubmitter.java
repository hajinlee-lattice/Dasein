package com.latticeengines.pls.workflow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.leadprioritization.workflow.PMMLModelWorkflowConfiguration;
import com.latticeengines.pls.service.MetadataFileUploadService;

@Component("pmmlModelWorkflowSubmitter")
public class PMMLModelWorkflowSubmitter extends BaseModelWorkflowSubmitter {
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(PMMLModelWorkflowSubmitter.class);

    @Autowired
    private MetadataFileUploadService metadataFileUploadService;

    public ApplicationId submit(String modelName, String moduleName, String pivotFileName, String pmmlFileName, SchemaInterpretation schemaInterpretation) {
        Map<String, Artifact> pmmlArtifacts = getArtifactMap(metadataFileUploadService.getArtifacts(moduleName,
                ArtifactType.PMML));
        Map<String, Artifact> pivotArtifacts = getArtifactMap(metadataFileUploadService.getArtifacts(moduleName,
                ArtifactType.PivotMapping));

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

        PMMLModelWorkflowConfiguration configuration = new PMMLModelWorkflowConfiguration.Builder()
                .podId(CamilleEnvironment.getPodId()) //
                .microServiceHostPort(microserviceHostPort) //
                .customer(getCustomerSpace()) //
                .workflow("pmmlModelWorkflow") //
                .modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                .modelName(modelName) //
                .pmmlArtifactPath(pmmlArtifact.getPath()) //
                .pivotArtifactPath(pivotArtifact != null ? pivotArtifact.getPath() : null) //
                .inputProperties(inputProperties) //
                .internalResourceHostPort(internalResourceHostPort) //
                .sourceSchemaInterpretation(schemaInterpretation.name()) //
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
