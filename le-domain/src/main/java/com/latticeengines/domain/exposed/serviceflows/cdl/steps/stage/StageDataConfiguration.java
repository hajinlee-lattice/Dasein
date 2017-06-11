package com.latticeengines.domain.exposed.serviceflows.cdl.steps.stage;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DataFlowStepConfiguration;

public class StageDataConfiguration extends DataFlowStepConfiguration {
    
    @NotNull
    private SourceFile sourceFile;
    
    public StageDataConfiguration() {
        super.setBeanName("createStagingTable");
    }

    public SourceFile getSourceFile() {
        return sourceFile;
    }

    public void setSourceFile(SourceFile sourceFile) {
        this.sourceFile = sourceFile;
    }

}
