package com.latticeengines.domain.exposed.serviceflows.cdl.steps.resolve;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DataFlowStepConfiguration;

public class ResolveDataConfiguration extends DataFlowStepConfiguration {
    
    @NotNull
    private SourceFile sourceFile;
    
    public ResolveDataConfiguration() {
        super.setBeanName("resolveStagingAndRuntimeTable");
    }

    public SourceFile getSourceFile() {
        return sourceFile;
    }

    public void setSourceFile(SourceFile sourceFile) {
        this.sourceFile = sourceFile;
    }

}
