package com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportStepConfiguration;

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
