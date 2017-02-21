package com.latticeengines.cdl.workflow.steps.stage;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.serviceflows.workflow.dataflow.DataFlowStepConfiguration;

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
