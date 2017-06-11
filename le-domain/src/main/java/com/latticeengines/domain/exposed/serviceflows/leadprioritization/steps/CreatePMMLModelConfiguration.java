package com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps;

import com.latticeengines.domain.exposed.serviceflows.core.steps.ModelStepConfiguration;

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
