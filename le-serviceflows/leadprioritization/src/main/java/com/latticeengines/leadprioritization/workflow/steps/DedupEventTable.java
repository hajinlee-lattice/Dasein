package com.latticeengines.leadprioritization.workflow.steps;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.SourceFileState;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("dedupEventTable")
public class DedupEventTable extends RunDataFlow<DedupEventTableConfiguration> {
    private static final Log log = LogFactory.getLog(DedupEventTable.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    public void execute() {
        log.info("Inside DedupEventTable execute()");

        super.execute();
        String tableName = getConfiguration().getName();
        updateSourceFile(tableName);
    }

    private void updateSourceFile(String tableName) {
        SourceFile sourceFile = retrieveSourceFile(getConfiguration().getCustomerSpace(), //
                getConfiguration().getSourceFileName());
        sourceFile.setTableName(tableName);
        sourceFile.setState(SourceFileState.Ready);
        getInternalResourceProxy().updateSourceFile(sourceFile, getConfiguration().getCustomerSpace().toString());
    }
}
