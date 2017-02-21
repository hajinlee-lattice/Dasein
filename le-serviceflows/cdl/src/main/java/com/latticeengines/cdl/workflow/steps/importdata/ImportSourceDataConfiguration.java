package com.latticeengines.cdl.workflow.steps.importdata;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.serviceflows.workflow.importdata.ImportStepConfiguration;

public class ImportSourceDataConfiguration extends ImportStepConfiguration {

    @NotNull
    private SourceFile sourceFile;
    
    public SourceFile getSourceFile() {
        return sourceFile;
    }

    public void setSourceFile(SourceFile sourceFile) {
        this.sourceFile = sourceFile;
    }


}
