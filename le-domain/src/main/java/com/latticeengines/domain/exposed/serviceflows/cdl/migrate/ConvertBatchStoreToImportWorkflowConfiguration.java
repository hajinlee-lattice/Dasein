package com.latticeengines.domain.exposed.serviceflows.cdl.migrate;

import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.ConvertBatchStoreStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.RegisterImportActionStepConfiguration;

public class ConvertBatchStoreToImportWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public ConvertBatchStoreToImportWorkflowConfiguration() {

    }

    public static class Builder {
        private ConvertBatchStoreToImportWorkflowConfiguration configuration =
                new ConvertBatchStoreToImportWorkflowConfiguration();

        private ConvertBatchStoreStepConfiguration convertBatchStoreStepConfiguration = new ConvertBatchStoreStepConfiguration();
        private RegisterImportActionStepConfiguration registerImportActionStepConfiguration = new RegisterImportActionStepConfiguration();
    }
}
