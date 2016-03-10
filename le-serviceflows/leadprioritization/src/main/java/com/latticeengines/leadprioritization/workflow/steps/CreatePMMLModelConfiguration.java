package com.latticeengines.leadprioritization.workflow.steps;

import com.latticeengines.serviceflows.workflow.modeling.ModelStepConfiguration;

public class CreatePMMLModelConfiguration extends ModelStepConfiguration {

    private String moduleName;
    private String pmmlArtifactPath;
    private String pivotArtifactPath;

    public String getModuleName() {
        return moduleName;
    }

    public void setModuleName(String moduleName) {
        this.moduleName = moduleName;
    }

    public String getPmmlArtifactPath() {
        return pmmlArtifactPath;
    }

    public void setPmmlArtifactPath(String pmmlArtifactPath) {
        this.pmmlArtifactPath = pmmlArtifactPath;
    }

    public String getPivotArtifactPath() {
        return pivotArtifactPath;
    }

    public void setPivotArtifactPath(String pivotArtifactPath) {
        this.pivotArtifactPath = pivotArtifactPath;
    }

}
