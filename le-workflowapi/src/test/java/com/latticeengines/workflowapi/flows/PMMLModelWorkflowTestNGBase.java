package com.latticeengines.workflowapi.flows;

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.leadprioritization.workflow.PMMLModelWorkflowConfiguration;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;

public class PMMLModelWorkflowTestNGBase extends WorkflowApiFunctionalTestNGBase {

    protected static final CustomerSpace PMML_CUSTOMERSPACE = CustomerSpace.parse("PmmlContract.PmmlTenant.Production");
    
    private String pmmlHdfsPath = null;
    private String pivotValuesHdfsPath = null;

    protected void setupForPMMLModel() throws Exception {
        setupTenant(PMML_CUSTOMERSPACE);
        setupUsers(PMML_CUSTOMERSPACE);
        setupCamille(PMML_CUSTOMERSPACE);
        setupHdfs(PMML_CUSTOMERSPACE);
        setupFiles(PMML_CUSTOMERSPACE);
    }
    
    private void setupFiles(CustomerSpace customerSpace) throws Exception {
        URL pmmlFile = ClassLoader.getSystemResource("com/latticeengines/workflowapi/flows/leadprioritization/pmmlfiles/rfpmml.xml");
        URL pivotFile =  ClassLoader.getSystemResource("com/latticeengines/workflowapi/flows/leadprioritization/pivotfiles/pivotvalues.txt");
        
        Path pmmlFolderHdfsPath = PathBuilder.buildMetadataPathForArtifactType(CamilleEnvironment.getPodId(), //
                customerSpace, "module1", ArtifactType.PMML);
        Path pivotValuesFolderHdfsPath = PathBuilder.buildMetadataPathForArtifactType(CamilleEnvironment.getPodId(), //
                customerSpace, "module1", ArtifactType.PivotMapping);
        
        pmmlHdfsPath = pmmlFolderHdfsPath.toString() + "/" + new File(pmmlFile.getFile()).getName();
        pivotValuesHdfsPath = pivotValuesFolderHdfsPath.toString() +"/" + new File(pivotFile.getFile()).getName();
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, pmmlFile.getPath(), pmmlHdfsPath);
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, pivotFile.getPath(), pivotValuesHdfsPath);
    }

    protected PMMLModelWorkflowConfiguration generatePMMLModelWorkflowConfiguration() {
        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "pmmlModelWorkflow");

        PMMLModelWorkflowConfiguration workflowConfig = new PMMLModelWorkflowConfiguration.Builder() //
                .podId(CamilleEnvironment.getPodId()) //
                .microServiceHostPort(microServiceHostPort) //
                .customer(PMML_CUSTOMERSPACE) //
                .workflow("pmmlModelWorkflow") //
                .modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                .modelName("PMMLModel-" + System.currentTimeMillis()) //
                .pmmlArtifactPath(pmmlHdfsPath) //
                .pivotArtifactPath(pivotValuesHdfsPath) //
                .inputProperties(inputProperties) //
                .internalResourceHostPort(internalResourceHostPort) //
                .build();

        return workflowConfig;
    }

}
