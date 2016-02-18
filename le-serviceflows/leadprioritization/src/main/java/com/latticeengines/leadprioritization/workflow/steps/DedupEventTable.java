package com.latticeengines.leadprioritization.workflow.steps;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.SourceFile;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("dedupEventTable")
public class DedupEventTable extends RunDataFlow<DedupEventTableConfiguration> {
    private static final Log log = LogFactory.getLog(DedupEventTable.class);

    @Override
    public void execute() {
        log.info("Inside DedupEventTable execute()");

        super.execute();
        updateSourceFile();
    }

    private void updateSourceFile() {
        SourceFile sourceFile = retrieveSourceFile(getConfiguration().getCustomerSpace(), //
                getConfiguration().getSourceFileName());
        sourceFile.setTableName(getConfiguration().getName());
        getInternalResourceProxy().updateSourceFile(sourceFile, getConfiguration().getCustomerSpace().toString());
    }
}
